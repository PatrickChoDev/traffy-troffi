from dagster import asset, AssetExecutionContext, Definitions, resource, job
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import time

@asset(required_resource_keys={"s3"},
        group_name="process_web_scrap_process",
        compute_kind="json",
        description="Process scraping the website"
)
def process_web_scrap(context: AssetExecutionContext):

    processed_folder = "traffy/processed"
    raw_folder ="traffy/raw"

    bangkok_wiki_scrap(context,raw_folder, processed_folder)
    green_process(context,raw_folder, processed_folder )
    pm_process(context,raw_folder, processed_folder)
    traffic_process(context,raw_folder, processed_folder)
    traffic_congress_process(context,raw_folder, processed_folder)
    waste_process(context,raw_folder, processed_folder)
@job(
    description="Process scraping the website",
)
def process_web_scrap_job():
    process_web_scrap

process_web_scrap_defs = Definitions(
    assets=[process_web_scrap],
    jobs=[process_web_scrap_job],
)

# https://data.bangkok.go.th/
def green_process(context, raw_folder, processed_folder):
    url = "https://data.bangkok.go.th/dataset/d161c1e4-e680-4aed-8be8-37c31046290a/resource/de49c4ca-95c7-405d-9cb6-b02aa11f3cfd/download/-9-.csv"
    df = fetch_csv_with_retries(url)
    _save_to_destinations(context, df, raw_folder, processed_folder, "fact_green")

def traffic_process(context, raw_folder, processed_folder):
    url = "https://data.bangkok.go.th/dataset/cf253ebf-ba2d-4a7d-97ce-9a03e559c4ef/resource/8f0c5a42-b86e-4a39-8d23-cc2ef9359c69/download/-16-6-66.csv"
    df = fetch_csv_with_retries(url)
    _save_to_destinations(context, df, raw_folder, processed_folder, "fact_traffic")

def pm_process(context, raw_folder, processed_folder):
    url = "https://data.bangkok.go.th/dataset/52a5da69-c086-425a-bcb3-fccfadd824f5/resource/b00b7694-f57e-4255-8971-0b26d4808cb3/download/20park_2564.csv"
    df = fetch_csv_with_retries(url)
    _save_to_destinations(context, df, raw_folder, processed_folder, "fact_pm")

def waste_process(context, raw_folder, processed_folder):
    url = "https://data.bangkok.go.th/dataset/58771fe2-7614-4ebc-ab0b-a85c84fe19be/resource/f08ef6ac-4c78-47c5-952c-46ffa6e93579/download/garbage.csv"
    df = fetch_csv_with_retries(url)
    _save_to_destinations(context, df, raw_folder, processed_folder, "fact_waste")

def traffic_congress_process(context, raw_folder, processed_folder):
    urls = [
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/f61c2b7c-7340-4754-9be0-f1a0fe280ac9/download/opendata-jan_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/3149417b-09bc-4637-9c00-624f944a6315/download/opendata-feb_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/4842b38c-5ff1-49e6-89fc-017cfe2935a3/download/opendata-mar_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/032906bb-3f62-442f-8f86-f13e32dfba42/download/opendata-apr_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/ba0188f4-d8bb-4d33-887c-f5f76731d78c/download/opendata-may_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/4cb55d09-2028-4d1f-89b4-ed5fba88caf3/download/opendata-jun_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/935a057b-094b-4f87-872b-081e09d41caa/download/opendata-jul_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/2f08aeb5-0f9f-480f-8e36-39ffc15cd458/download/opendata-aug_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/6064c40b-f8b6-4390-bc2b-da405aee18bf/download/opendata-sep_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/5c6017cf-3ac9-4cda-aace-7e3c76510df1/download/opendata-oct_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/5cd61444-2dde-4ae4-b2f7-7ea45f292a5e/download/opendata-nov_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/f90f0322-2a96-4a4e-827e-8f72ad451429/download/opendata-dec_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/afdb4797-ba79-4682-a73f-cc00035382e1/download/opendata-jan2024_ok.xlsx",
        "https://data.bangkok.go.th/dataset/dc9bea89-ca70-4b0f-aff4-8660857d1b13/resource/04b01f04-641e-41db-85e2-0a814e396292/download/opendata-feb.-2024_ok.xlsx"
    ]

    all_data = []

    for url in urls:
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                df = pd.read_excel(url)
                if 'No' in df.columns:
                    df = df.drop(columns=['No'])
                all_data.append(df)
                print(f"Loaded: {url}")
            else:
                print(f"Skipped (status {response.status_code}): {url}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to access {url}: {e}")
        time.sleep(0.5)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        _save_to_destinations(context, combined_df, raw_folder, processed_folder, "traffic_congress")
    else:
        print("No data collected due to errors in all URLs.")

def fetch_csv_with_retries(url, retries=5, delay=2):
    for attempt in range(1, retries + 1):
        try:
            return pd.read_csv(url)
        except Exception as e:
            print(f"[Attempt {attempt}] Failed to fetch CSV: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                raise RuntimeError(f"Failed to fetch CSV after {retries} attempts") from e

def _save_to_destinations(context, df, raw_folder, processed_folder, name):
    context.resources.s3.upload_parquet(f"{raw_folder}/parquet/{name}.parquet", df)
    context.resources.s3.upload_csv(f"{raw_folder}/csv/{name}.csv", df)

    df.to_csv(f"public/{name}.csv", index=False)

def bangkok_wiki_scrap(context,raw_folder, processed_folder):
    wikiData = requests.get('https://th.wikipedia.org/wiki/%E0%B8%A3%E0%B8%B2%E0%B8%A2%E0%B8%8A%E0%B8%B7%E0%B9%88%E0%B8%AD%E0%B9%80%E0%B8%82%E0%B8%95%E0%B8%82%E0%B8%AD%E0%B8%87%E0%B8%81%E0%B8%A3%E0%B8%B8%E0%B8%87%E0%B9%80%E0%B8%97%E0%B8%9E%E0%B8%A1%E0%B8%AB%E0%B8%B2%E0%B8%99%E0%B8%84%E0%B8%A3')
    soup = BeautifulSoup(wikiData.text, "lxml")
    tables = soup.find_all('table', {'role': 'presentation'})

    data = []

    base_url = "https://th.wikipedia.org"

    # Assuming you have a 'soup' variable containing the BeautifulSoup object of the main page
    tables = soup.find_all('table', {'role': 'presentation'})

    for table in tables:
        links = table.find_all('a')

        for link in links:
            href = link.get('href')
            title = link.get('title')
            if href and title:
                link_data = {}  # Initialize link_data for each link
                full_url = f"{base_url}{href}"
                try:
                    response = requests.get(full_url)
                    response.raise_for_status()  # Check if the request was successful
                    page_soup = BeautifulSoup(response.content, 'html.parser')

                    location_info = extract_location_info(page_soup)
                    if location_info:
                        link_data.update(location_info)  # Add location info to link_data

                        subdistrict_table = extract_subdistrict_table(page_soup)
                        if subdistrict_table:
                            # Collect the cross-joined data (a list of dictionaries)
                            cross_joined_data = cross_join(location_info, subdistrict_table)
                            # Instead of updating, append the cross-joined data to the link_data list
                            for subdistrict_info in cross_joined_data:
                                # For each subdistrict, create a new dictionary
                                subdistrict_link_data = link_data.copy()  # Copy the base location data
                                subdistrict_link_data.update(subdistrict_info)  # Add the subdistrict info
                                data.append(subdistrict_link_data)  # Append the updated dictionary

                        else:
                            # If no subdistrict data, append the link_data as is
                            data.append(link_data)

                        print(f"✅ {title}")
                    else:
                        print(f"⚠️ Skipped: {title} (no useful data)")

                    time.sleep(0.01)  # Increase the sleep time to avoid overloading the server

                except requests.exceptions.RequestException as e:
                    print(f"Error fetching {full_url}: {e}")
                    time.sleep(2)  # Sleep longer after an error to avoid too many retries quickly
    df = pd.DataFrame(data)

    _save_to_destinations(context, df, raw_folder, processed_folder, "bangkok_district")

def extract_location_info(page_soup):
    # Extract Thai Name
    thaiName_tag = page_soup.find('span', {'class': 'mw-page-title-main'})
    if not thaiName_tag:
        # If Thai name is not found, return None or some default value
        return None

    thaiName = thaiName_tag.text.strip()[3:]  # Extract the text and strip any leading/trailing whitespace

    # Initialize variables for other data
    EngName = None
    population = None
    density = None
    postal_code = None
    geocode = None
    year = None
    address = None

    # Find the infobox in the page
    infobox = page_soup.find('table', {'class': 'infobox geography vcard'})

    if infobox:
        # Iterate over each row in the infobox
        for row in infobox.find_all('tr'):
            text = row.get_text(separator=' ', strip=True)

            # Population
            if "ทั้งหมด" in text and "คน" in text:
                td = row.find('td')
                if td:
                    match = re.search(r'[\d,]+', td.get_text())
                    if match:
                        population = int(match.group(0).replace(',', ''))

            # Density
            density_match = re.search(r'ความหนาแน่น\s*([\d,\.]+)\s*คน/ตร\.กม\.', text)
            if density_match:
                density = density_match.group(1).replace(',', '')

            # Postal Code
            postal_match = re.search(r'รหัสไปรษณีย์\s*(\d{5})', text)
            if postal_match:
                postal_code = postal_match.group(1)

            # Geocode
            geo_match = re.search(r'รหัสภูมิศาสตร์\s*(\d+)', text)
            if geo_match:
                geocode = geo_match.group(1)

            # Year extraction
            th = row.find('th')
            if th and "ประชากร" in th.text:
                small = th.find('small')
                if small:
                    year = small.text.strip()

            # Address extraction
            if th and 'ที่อยู่' in th.text:
                td = row.find('td')
                if td:
                    address = td.text.strip()

            # English Name extraction
            if th and 'อักษรโรมัน' in th.text:
                td = row.find('td')
                if td:
                    EngName = td.text.strip()[5:]

    # List of categories to extract (ID based), allowing for multiple terms
    categories = {
        "วัด|มัสยิด|ศาลเจ้า": "District_Place_of_worship", 
        "วัง|อนุสาวรีย์": "District_Cultural_heritage", 
        "สถานที่สำคัญ": "District_Important_Place",
        "โรงเรียน|มหาวิทยาลัย|วิทยาลัยของรัฐ|สถาบันการศึกษา": "District_Education_location", 
        "ตลาด|ศูนย์การค้า": "District_Commercial_areas", 
        "ราชการ|หน่วยงาน": "District_Agency",
        "คมนาคม": "District_Transportation"
    }

    # Initialize lists for categories
    place_of_worship = []
    education_location = []
    cultural_heritage = []
    commercial_areas = []
    transportation = []
    agency = []

    # Loop through the categories and extract corresponding <ul> lists
    for category_terms, category_field in categories.items():
        # Create a regex pattern from the terms, separated by "|"
        category_pattern = re.compile(category_terms)

        # Search for the category
        for h3_tag in page_soup.find_all("h3"):
            if category_pattern.search(h3_tag.text):
                next_ul = h3_tag.find_next("ul")
                if next_ul:
                    for li in next_ul.find_all("li"):
                        text = li.get_text(strip=True)

                        # Append the information to the correct category list
                        if category_field == "District_Place_of_worship":
                            place_of_worship.append(text)
                        elif category_field == "District_Education_location":
                            education_location.append(text)
                        elif category_field == "District_Cultural_heritage":
                            cultural_heritage.append(text)
                        elif category_field == "District_Commercial_areas":
                            commercial_areas.append(text)
                        elif category_field == "District_Transportation":
                            transportation.append(text)
                        elif category_field == "District_Agency":
                            agency.append(text)

        for h2_tag in page_soup.find_all("h2"):  # Changed to find <h2> tags
            if category_pattern.search(h2_tag.text):  # Match the category pattern
                next_ul = h2_tag.find_next("ul")  # Find the <ul> after the <h2>
                if next_ul:
                    for li in next_ul.find_all("li"):  # Loop through all <li> in the <ul>
                        text = li.get_text(strip=True)

                        # Append the information to the correct category list
                        if category_field == "District_Place_of_worship":
                            place_of_worship.append(text)
                        elif category_field == "District_Education_location":
                            education_location.append(text)
                        elif category_field == "District_Cultural_heritage":
                            cultural_heritage.append(text)
                        elif category_field == "District_Commercial_areas":
                            commercial_areas.append(text)
                        elif category_field == "District_Transportation":
                            transportation.append(text)
                        elif category_field == "District_Agency":
                            agency.append(text)

    return {
        "District_Thai_Name": thaiName,
        "District_English_Name": EngName,
        "District_Postal_Code": postal_code,
        "District_Geocode": geocode,
        "District_Office_Address": address,
        "District_Population_2566": population,
        "District_Density_2566": density,
        "District_Place_of_worship": place_of_worship,
        "District_Education_location": education_location,
        "District_Cultural_heritage": cultural_heritage,
        "District_Commercial_areas": commercial_areas,
        "District_Transportation": transportation,
        "District_Agency": agency   
    }

def extract_subdistrict_table(page_soup):
    # ค้นหาตารางที่มีแขวง/ตำบล (โดยปกติเป็น class="wikitable")
    sub = page_soup.find('table', {'class': 'wikitable'})
    if not sub:
        return None  # ถ้าไม่เจอ

    # ดึงหัวตาราง
    headers = [header.text.strip() for header in sub.find_all('th')][1:6]
    if not headers:
        return None  # ป้องกันกรณีไม่มีหัวตาราง

    rows = []
    for row in sub.find_all('tr')[1:]:  # ข้ามแถวหัวตาราง
        cells = row.find_all('td')
        if len(cells) > 0:
            # Extract the data, skipping the first column and limiting to the next 5 columns
            data = {headers[i]: cells[i+1].text.strip() for i in range(min(len(headers), len(cells)-1))}
            rows.append(data)

    # Assuming you want to extract the values specifically into a dict
    subdistrict_data = []
    for row in rows:
        subdistrict_info = {
            "SubDistrict_Thai_Name": row.get("อักษรไทย", ""),
            "SubDistrict_English_Name": row.get("อักษรโรมัน", ""),
            "SubDistrict_Area": row.get("พื้นที่ (ตร.กม.)", ""),
            "SubDistrict_Population_2566": row.get("จำนวนประชากร (ธันวาคม 2566)", ""),
            "SubDistrict_Density_2566": row.get("ความหนาแน่นประชากร (ธันวาคม 2566)", "")
        }
        subdistrict_data.append(subdistrict_info)

    return subdistrict_data

def cross_join(location_data, subdistrict_data):
    # Create a list to hold the cross-joined data
    cross_joined_data = []

    # If there is no subdistrict data, return an empty list
    if not subdistrict_data:
        return []

    # Perform cross join between location data and each subdistrict's data
    for district in [location_data]:  # Only one location data, so wrap it in a list
        for subdistrict in subdistrict_data:
            # Combine location data with each subdistrict's data (cross-join)
            combined_data = {**district, **subdistrict}  # Merge the two dictionaries
            cross_joined_data.append(combined_data)

    return cross_joined_data
