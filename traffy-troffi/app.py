import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
from psycopg2 import sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from functools import reduce
import pydeck as pdk
import plotly.express as px
import folium
from streamlit_folium import st_folium
from geopy.geocoders import Nominatim
import random
import string

import logging
from typing import Optional

from pyspark.sql import SparkSession
import os

import psycopg2

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_params = {
    'dbname': 'traffy-troffi',
    'user': 'postgres',
    'password': 'troffi',
    'host': 'localhost',
    'port': '5432'
}


def insert_complaint_to_db(complaint, image_path, image_after_path, latitude, longitude):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # Execute INSERT query
        query = sql.SQL("""
                        INSERT INTO traffy_fondue_untagged
                        (ticket_id, complaint, image, image_after, latitude, longitude, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        RETURNING ticket_id;
                        """)
        characters = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
        random_string = ''.join(random.choice(characters) for _ in range(18))
        cursor.execute(query, (
            random_string,
            complaint,
            image_path,
            image_after_path,
            latitude,
            longitude
        ))

        # Get the inserted ID
        complaint_id = cursor.fetchone()[0]

        # Commit the transaction
        conn.commit()

        return complaint_id

    except Exception as e:
        st.error(f"เกิดข้อผิดพลาด: {e}")
        return None

    finally:
        # Close connections
        if cursor:
            cursor.close()
        if conn:
            conn.close()


class SimpleSparkSession:
    """Simple Spark session builder for Jupyter notebooks"""

    def __init__(
            self,
            app_name="Jupyter Spark Session",
            master="local[*]",
            spark_config=None,
            enable_hive_support=False,
            # S3 configuration
            s3_bucket_name=None,
            s3_endpoint=None,
            s3_access_key=None,
            s3_secret_key=None,
            s3_region="us-east-1",
            s3_path_style_access=True,
            # PostgreSQL configuration
            postgres_config=None,
            # Package configuration
            packages=None
    ):
        self.app_name = app_name
        self.master = master
        self.spark_config = spark_config or {}
        self.enable_hive_support = enable_hive_support

        # S3 config
        self.s3_bucket_name = s3_bucket_name
        self.s3_endpoint = s3_endpoint
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_region = s3_region
        self.s3_path_style_access = s3_path_style_access

        # PostgreSQL config
        self.postgres_config = postgres_config
        self.jdbc_driver_path: Optional[str] = None

        # Packages
        self.packages = packages or []

        self._session = None

    def build_session(self):
        """Build and return a SparkSession"""
        if self._session is not None:
            return self._session

        # Start building the session
        builder = SparkSession.builder.appName(self.app_name).master(self.master)

        builder = builder \
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .config("spark.driver.memory", "8g")

        # Add Hive support if requested
        if self.enable_hive_support:
            builder = builder.enableHiveSupport()

        if self.jdbc_driver_path:
            builder = builder.config("spark.driver.extraClassPath", self.jdbc_driver_path)
            builder = builder.config("spark.executor.extraClassPath", self.jdbc_driver_path)

        # Add all configuration options
        for key, value in self.spark_config.items():
            builder = builder.config(key, value)

        # Configure packages
        if self.packages:
            packages = ",".join(self.packages)
            builder = builder.config("spark.jars.packages", packages)

        # Add S3 configuration if credentials provided
        if self.s3_access_key and self.s3_secret_key:
            builder = builder.config("spark.hadoop.fs.s3a.access.key", self.s3_access_key)
            builder = builder.config("spark.hadoop.fs.s3a.secret.key", self.s3_secret_key)
            builder = builder.config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

            # Config for non-AWS S3
            if self.s3_endpoint:
                builder = builder.config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint)
                builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", self.s3_region)

            # Path style access for non-AWS implementations
            if self.s3_path_style_access:
                builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
                builder = builder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                builder = builder.config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")

        # Build the session
        logger.info(f"Building Spark session with app name: {self.app_name}, master: {self.master}")
        self._session = builder.getOrCreate()

        return self._session

    def get_session(self):
        """Get the current SparkSession or create a new one"""
        return self.build_session()

    def stop_session(self):
        """Stop the current Spark session if it exists"""
        if self._session is not None:
            self._session.stop()
            self._session = None
            logger.info("Spark session stopped")


# ---------- 1. สร้าง SparkSession ----------
@st.cache_resource
def get_spark():
    return SimpleSparkSession(
        app_name="Data Analysis Notebook",
        packages=[
            "org.postgresql:postgresql:42.5.4",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.426"
        ],
        s3_access_key=os.getenv("S3_ACCESS_KEY"),
        s3_secret_key=os.getenv("S3_SECRET_KEY"),
        s3_endpoint=os.getenv("S3_ENDPOINT"),
        s3_region="garage",
        s3_path_style_access=True,
        postgres_config={
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "driver": "org.postgresql.Driver",
            "currentSchema": "public"
        },
        enable_hive_support=False,
        s3_bucket_name="traffy-troffi"
    ).get_session()


spark = get_spark()


# ---------- 2. โหลดข้อมูลหลัก ----------
@st.cache_resource
def load_data():
    df = spark.read.jdbc(
        table='traffy_fondue',
        url="jdbc:postgresql://localhost:5432/traffy-troffi",
        properties={
            "user": "postgres",
            "password": "troffi",
            "driver": "org.postgresql.Driver",
            "currentSchema": "public"
        }
    )
    return df.cache()


df = load_data()


# ---------- 3. ดึงค่าทั้งหมดของ district และ category ----------
@st.cache_data(ttl=30)
def get_filter_values(_df):
    districts = [r[0] for r in _df.select("district").distinct().orderBy("district").collect()]
    categories = [r[0] for r in _df.selectExpr("explode(categories) as cat").distinct().orderBy("cat").collect()]
    return districts, categories


districts, categories = get_filter_values(df)


# ---------- 5. ดึง subdistrict เฉพาะของ district นั้น ----------
@st.cache_data(ttl=30)
def get_subdistricts_by_district(_df, selected_district):
    result = _df.filter(F.col("district") == selected_district) \
        .select("subdistrict").distinct().orderBy("subdistrict") \
        .collect()
    return [r[0] for r in result]


with st.sidebar:
    st.header("🔍 ตัวกรองข้อมูล")

    selected_district = st.selectbox("เลือกเขต (District)", districts)

    # ดึง subdistrict เฉพาะของ district นั้น
    subdistricts = get_subdistricts_by_district(df, selected_district)
    selected_subdistrict = st.selectbox("เลือกแขวง (Subdistrict)", subdistricts)

    selected_categories = st.multiselect("เลือกประเภท (Categories)", categories)

    st.markdown("---")
    st.header("📝 เพิ่มข้อมูลปัญหาใหม่")

    complaint = st.text_area("รายละเอียดปัญหา (Complaint)")
    image = st.text_input("ลิงก์รูปภาพก่อนแก้ไข (Image URL)")
    image_after = st.text_input("ลิงก์รูปภาพหลังแก้ไข (Image After URL)")

    # Default พิกัด
    default_lat = 13.7563
    default_lon = 100.5018

    st.markdown("**📍 คลิกแผนที่เพื่อเลือกตำแหน่ง**")

    # ตรวจสอบว่าเคยมีการคลิกหรือยัง (session state)
    if 'lat' not in st.session_state:
        st.session_state.lat = default_lat
        st.session_state.lon = default_lon

    # สร้างแผนที่พร้อมหมุด (ถ้ามี)
    m = folium.Map(location=[st.session_state.lat, st.session_state.lon], zoom_start=12)

    # เพิ่มหมุดถ้ามีพิกัดที่เลือกแล้ว
    if st.session_state.lat != default_lat or st.session_state.lon != default_lon:
        folium.Marker(
            location=[st.session_state.lat, st.session_state.lon],
            popup="ตำแหน่งที่เลือก",
            tooltip="ตำแหน่งที่เลือก",
            icon=folium.Icon(color="red")
        ).add_to(m)

    # รับ interaction จากแผนที่
    map_data = st_folium(m, height=350, width=600, key="map_with_marker", returned_objects=["last_clicked"])

    # ตรวจสอบว่ามีการคลิกบนแผนที่หรือไม่
    if map_data and "last_clicked" in map_data and map_data["last_clicked"] is not None:
        # อัพเดทพิกัดใน session state
        st.session_state.lat = map_data["last_clicked"]["lat"]
        st.session_state.lon = map_data["last_clicked"]["lng"]
        st.rerun()  # รีรันเพื่ออัพเดทแผนที่พร้อมหมุด

    # แสดงข้อความตามสถานะ
    if st.session_state.lat != default_lat or st.session_state.lon != default_lon:
        st.success(f"✅ พิกัดที่เลือก: {st.session_state.lat:.6f}, {st.session_state.lon:.6f}")
    else:
        st.info("🖱 กรุณาคลิกที่แผนที่เพื่อเลือกตำแหน่ง")

    # ช่องแก้ไข lat/lon เพิ่มเติม
    latitude = st.number_input("Latitude", value=st.session_state.lat, format="%.6f", key="lat_input")
    longitude = st.number_input("Longitude", value=st.session_state.lon, format="%.6f", key="lon_input")

    # อัพเดท session state ถ้ามีการเปลี่ยนแปลงค่าใน input
    if latitude != st.session_state.lat or longitude != st.session_state.lon:
        st.session_state.lat = latitude
        st.session_state.lon = longitude
        st.rerun()  # รีรันเพื่ออัพเดทแผนที่

    if st.button("ส่งข้อมูล"):

        # Insert to database
        complaint_id = insert_complaint_to_db(complaint, image, image_after, latitude, longitude)

        if complaint_id:
            st.success("ส่งข้อมูลเรียบร้อย")

            # Display submitted data
            st.write({
                "complaint": complaint,
                "image": image,
                "image_after": image_after,
                "latitude": latitude,
                "longitude": longitude,
                "complaint_id": complaint_id
            })
st.header(f"เขต {selected_district} แขวง {selected_subdistrict}")
# ---------- 7. Filter ข้อมูล ----------
filtered_df = df.filter(
    (F.col("district") == selected_district) &
    (F.col("subdistrict") == selected_subdistrict)
)

# ถ้ามีการเลือก category → ต้องให้ตรงกับอย่างน้อยหนึ่งใน array
if selected_categories:
    conditions = [F.array_contains(F.col("categories"), cat) for cat in selected_categories]
    filtered_df = filtered_df.filter(reduce(lambda a, b: a | b, conditions))


@st.cache_data
def load_district_info():
    return pd.read_csv("./public/dim_district.csv")


@st.cache_data
def load_subdistrict_info():
    return pd.read_csv("./public/dim_subdistrict.csv")


district_info_df = load_district_info()
subdistrict_info_df = load_subdistrict_info()

# ---------- 8. แสดงผล ----------
tab1, tab2 = st.tabs(["📊 ข้อมูลทั่วไปของเขต", "🛠️ รายงานปัญหา"])
with tab1:
    # filter ข้อมูลเขตจาก CSV
    district_row = district_info_df[district_info_df["district_name"] == selected_district]

    if not district_row.empty:
        row = district_row.iloc[0]  # เอาแถวแรกออกมาเป็น Series
        st.subheader(f"แขวง {selected_district}")
        st.markdown(f"""
        **ชื่อภาษาอังกฤษ:** {row['district_english_name']}
        **รหัสเขต (geocode):** {row['district_geocode']}
        **รหัสไปรษณีย์:** {row['district_postal_code']}
        **ที่อยู่สำนักงานเขต:** {row['district_office_address']}
        """)

    subdistrict_row = subdistrict_info_df[
        (subdistrict_info_df["district_name"] == selected_district) &
        (subdistrict_info_df["subdistrict_name"] == selected_subdistrict)
        ]

    if not subdistrict_row.empty:
        row = subdistrict_row.iloc[0]

        st.markdown("---")
        st.subheader(f"แขวง {selected_subdistrict}")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**ชื่อภาษาอังกฤษ:** {row['subdistrict_english_name']}")
            st.markdown(f"**พื้นที่:** {row['subdistrict_area']} ตร.กม.")
        with col2:
            st.markdown(f"**ประชากร (ปี 2566):** {row['subdistrict_population_2566']:} คน")
            st.markdown(f"**ความหนาแน่น (ปี 2566):** {row['subdistrict_density_2566']} คน/ตร.กม.")


    @st.cache_data
    def load_pm_data():
        return pd.read_csv("./public/fact_pm.csv")


    pm_df = load_pm_data()
    pm_row = pm_df[pm_df["district_name"] == selected_district]

    if not pm_row.empty:
        row = pm_row.iloc[0]

        st.subheader("💨 สถานการณ์ฝุ่น PM2.5 ในเขตนี้ (เฉลี่ยรายเดือน)")

        pm_avg = {
            "ม.ค.": row["jan_average_PM2.5"],
            "ก.พ.": row["feb_average_PM2.5"],
            "มี.ค.": row["mar_average_PM2.5"],
            "เม.ย.": row["apr_average_PM2.5"],
            "พ.ค.": row["may_average_PM2.5"],
            "มิ.ย.": row["jun_average_PM2.5"],
            "ก.ค.": row["jul_average_PM2.5"],
            "ส.ค.": row["aug_average_PM2.5"],
            "ก.ย.": row["sep_average_PM2.5"],
            "ต.ค.": row["oct_average_PM2.5"],
            "พ.ย.": row["nov_average_PM2.5"],
            "ธ.ค.": row["dec_average_PM2.5"],
        }

        # แปลง dict เป็น DataFrame
        pm_avg_df = pd.DataFrame({
            "เดือน": list(pm_avg.keys()),
            "ค่าเฉลี่ย PM2.5": list(pm_avg.values())
        })

        # ใช้ plotly เพื่อควบคุม Y scale
        fig = px.line(
            pm_avg_df,
            x="เดือน",
            y="ค่าเฉลี่ย PM2.5",
            markers=True,
            title=f"PM2.5 รายเดือนของเขต {selected_district}"
        )
        fig.update_layout(
            yaxis_title="μg/m³",
            xaxis_title="เดือน",
            template="simple_white"
        )

        st.plotly_chart(fig, use_container_width=True)


    @st.cache_data
    def load_traffic_data():
        return pd.read_csv("./public/fact_traffic.csv")


    traffic_df = load_traffic_data()

    # filter เฉพาะเขตนี้ ถ้าระบุได้จาก 'location_traffic_light'
    traffic_filtered = traffic_df[traffic_df["location_traffic_light"].str.contains(selected_district)]

    if not traffic_filtered.empty:
        st.subheader("🚦 ตำแหน่งไฟจราจรในเขตนี้")
        st.map(traffic_filtered.rename(columns={"lat": "latitude", "long": "longitude"}))


    # โหลดไฟล์ bangkok_district.csv แค่ครั้งเดียว (แนะนำให้ใช้ @st.cache_data ถาวร)
    @st.cache_data
    def load_bangkok_district_info():
        return pd.read_csv("./public/bangkok_district.csv")


    district_info_df = load_bangkok_district_info()

    # ดึงเฉพาะข้อมูลของเขตที่เลือก
    district_row = district_info_df[district_info_df["District_Thai_Name"] == selected_district]


    def clean_value(val):
        if pd.isnull(val) or str(val).strip() in ["[]", "", "na", "NA", "-", "NaN"]:
            return "ไม่มี"
        return val


    if not district_row.empty:
        row = district_row.iloc[0]

        st.markdown("### 🏙️ ข้อมูลสถานที่สำคัญในเขตนี้")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown(f"**📍 ศาสนสถาน (วัด ฯลฯ):** {clean_value(row['District_Place_of_worship'])}")
            st.markdown(f"**🏫 สถานศึกษา:** {clean_value(row['District_Education_location'])}")
            st.markdown(f"**🏛 มรดกวัฒนธรรม:** {clean_value(row['District_Cultural_heritage'])}")

        with col2:
            st.markdown(f"**🛍 พื้นที่พาณิชย์:** {clean_value(row['District_Commercial_areas'])}")
            st.markdown(f"**🚇 ระบบขนส่ง:** {clean_value(row['District_Transportation'])}")

with tab2:
    st.subheader("ผลลัพธ์ (แสดงสูงสุด 100 รายการ):")
    st.dataframe(filtered_df.limit(100).toPandas())

    st.subheader(f"สัดส่วนประเภทปัญหาในเขต {selected_district}")

    df_exploded = df.filter(F.col("district") == selected_district) \
        .selectExpr("explode(categories) as category")

    df_category_count = df_exploded.groupBy("category").count().orderBy(F.desc("count"))
    category_pd = df_category_count.toPandas()

    if not category_pd.empty:
        st.bar_chart(category_pd.set_index("category"))
    else:
        st.info("ไม่มีข้อมูลประเภทในเขตนี้")

    st.subheader(f"แผนที่ปัญหา Top 3 หมวดในเขต {selected_district}")

    # 1. Filter เฉพาะเขต และมี lat/lon
    df_district_latlon = df.filter(
        (F.col("district") == selected_district) &
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull()
    )

    # 2. ระเบิด category แล้วนับ Top 3
    df_exploded = df_district_latlon.selectExpr("explode(categories) as category", "*")
    top_categories = [r["category"] for r in df_exploded.groupBy("category")
    .count().orderBy(F.desc("count")).limit(3).collect()]

    # 3. ดึงเฉพาะปัญหาที่อยู่ใน Top 3 categories
    df_map = df_exploded.filter(
        (F.col("district") == selected_district) &
        F.col("latitude").isNotNull() &
        F.col("longitude").isNotNull()
    ).select("latitude", "longitude", "category", "complaint", "image", "image_after") \
        .withColumnRenamed("category", "Category") \
        .withColumnRenamed("complaint", "Description") \
        .withColumnRenamed("image", "BeforeImage") \
        .withColumnRenamed("image_after", "AfterImage")

    # 4. Convert to Pandas
    df_map_pd = df_map.toPandas()

    base_colors = [
        [255, 0, 0],  # แดง
        [0, 128, 0],  # เขียว
        [0, 0, 255],  # น้ำเงิน
    ]
    default_color = [160, 160, 160]  # เทา

    # ป้องกัน IndexError โดยวนจับคู่จาก top_categories กับ base_colors เท่าที่มี
    color_map = {
        cat: base_colors[i] for i, cat in enumerate(top_categories)
    }

    df_map_pd["color"] = df_map_pd["Category"].apply(lambda cat: color_map.get(cat, default_color))

    st.markdown("#### 🟢 หมวดหมู่ยอดนิยมในเขตนี้:")
    for cat, color in color_map.items():
        color_hex = '#%02x%02x%02x' % tuple(color)
        st.markdown(f"- <span style='color:{color_hex}'>⬤</span> {cat}", unsafe_allow_html=True)

    tooltip = {
        "html": """
        <div style="max-width: 320px; font-size: 12px;">
            <b>หมวดหมู่:</b> {Category}<br/>
            <b>รายละเอียด:</b> {Description}<br/>
            <div style="display: flex; gap: 4px; margin-top: 5px;">
                <div>
                    <div style="font-size: 11px; margin-bottom: 2px;">ก่อนแก้:</div>
                    <img src="{BeforeImage}" width="140" style="border:1px solid #ccc;"/>
                </div>
                <div>
                    <div style="font-size: 11px; margin-bottom: 2px;">หลังแก้:</div>
                    <img src="{AfterImage}" width="140" style="border:1px solid #ccc;"/>
                </div>
            </div>
        </div>
        """,
        "style": {
            "backgroundColor": "white",
            "color": "black"
        }
    }

    # 5. ถ้ามีข้อมูล → แสดงแผนที่
    if not df_map_pd.empty:
        view_state = pdk.ViewState(
            latitude=df_map_pd["latitude"].mean(),
            longitude=df_map_pd["longitude"].mean(),
            zoom=12,
        )

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df_map_pd,
            get_position='[longitude, latitude]',
            get_color="color",
            get_radius=20,
            pickable=True,
        )

        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=view_state,
            layers=[layer],
            tooltip=tooltip
        ))

    else:
        st.info("ไม่มีข้อมูลพิกัดของปัญหาในเขตนี้")
