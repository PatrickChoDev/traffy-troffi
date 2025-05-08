import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from functools import reduce
import pydeck as pdk
import plotly.express as px
import folium
from streamlit_folium import st_folium

# ---------- 1. สร้าง SparkSession ----------
@st.cache_resource
def get_spark():
    return SparkSession.builder \
        .appName("Traffy Troffi Viewer") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.27") \
        .getOrCreate()

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
@st.cache_data(ttl=300)
def get_filter_values(_df):
    districts = [r[0] for r in _df.select("district").distinct().orderBy("district").collect()]
    categories = [r[0] for r in _df.selectExpr("explode(categories) as cat").distinct().orderBy("cat").collect()]
    return districts, categories

districts, categories = get_filter_values(df)





# ---------- 5. ดึง subdistrict เฉพาะของ district นั้น ----------
@st.cache_data(ttl=300)
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
        [255, 0, 0],    # แดง
        [0, 128, 0],    # เขียว
        [0, 0, 255],    # น้ำเงิน
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
