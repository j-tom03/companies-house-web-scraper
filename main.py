import time, random, re, sys, httpx

import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from tqdm import tqdm
from googlesearch import search

# cache to speed up geocode searching
geocode_cache = {}

def geocode_postcode(postcode: str) -> tuple:
    """Geocodes a given postcode returning coordinates"""
    if postcode in geocode_cache:
        return geocode_cache[postcode]
    geolocator = Nominatim(user_agent="postcode_distance_calculator", timeout=10)
    try:
        location = geolocator.geocode(postcode)
        if location:
            geocode_cache[postcode] = (location.latitude, location.longitude)
            return geocode_cache[postcode]
    except Exception as e:
        print(f"Error geocoding {postcode}: {e}")
    return None

def calculate_distance(row: pd.Series, base_location: tuple) -> float:
    """Calculates distance of datapoint from base location"""
    try:
        location = geocode_postcode(row["postcode"])
    except:
        print("No 'postcode' column found - file invalid")
        return 
    if location:
        return geodesic(base_location, location).miles
    return np.nan

def extract_postcode(row: pd.Series) -> str:
    """Uses regex to extract a postcode from a pandas row"""

    try:
        address = row["registered_office_address"]
    except:
        print("No 'registered_office_address' found - file invalid")
        return
    
    postcode_pattern = r'\b[A-Z]{1,2}[0-9][0-9A-Z]?\s[0-9][A-Z]{2}\b'
    
    match = re.search(postcode_pattern, address)
    if match:
        postcode = match.group()
        return postcode
    else:
        # if no postcode return a blank string
        return ""

def add_postcode_col(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a postcode column to the dataframe and returns the df"""

    df["postcode"] = df.apply(extract_postcode, axis=1)
    return df

def add_dist_col(filename: str, base_postcode: str) -> bool:
    """Adds a distance column to the spreadsheet"""

    # checking which filetype to read with
    try:
        if filename.lower().endswith(".csv"):
            df = pd.read_csv(filename)
        elif filename.lower().endswith(".xls"):
            df = pd.read_excel(filename)
        elif filename.lower().endswith(".xlsx"):
            df = pd.read_excel(filename)
        elif filename.lower().endswith(".xlsm"):
            df = pd.read_excel(filename)
        else:
            print("Invalid file type")
            return False
    except:
        print("File does not exist")
        return False
    
    try:
        base_location = geocode_postcode(base_postcode)
    except:
        print("Base postcode not valid")
        return False
    if not base_location:
        raise ValueError(f"Could not geocode base postcode: {base_postcode}")
    
    print("Adding a postcode column")
    df = add_postcode_col(df)

    # Apply distance calculation with a progress bar
    tqdm.pandas(desc="Calculating distances")
    df["distance_miles"] = df.progress_apply(calculate_distance, args=(base_location,), axis=1)

    # Sort the DataFrame by distance
    df.sort_values(by=['distance_miles'], inplace=True)

    if filename.lower().endswith(".csv"):
        df.to_csv(filename, index=False)
    elif filename.lower().endswith(".xls"):
        df.to_excel(filename, index=False)
    elif filename.lower().endswith(".xlsx"):
        df.to_excel(filename, index=False)
    elif filename.lower().endswith(".xlsm"):
        df.to_excel(filename, index=False)

    print(f"File saved as: {filename}")
    return True

def find_url(row: pd.Series, num_results=5):
    # setting random timeout to stop request blocks
    time.sleep(random.randint(1,10))

    try:
        company_name = row["company_name"]
    except:
        print("No 'company_name' column found - file invalid")
        return 
    
    query = f"{company_name}"
    try:
        # searches query and returns the top valid URL
        results = list(search(query, num_results=num_results))
        for url in results:
            # Skip URLs from companies house itself
            if not url.startswith("https://find-and-update.company-information.service.gov.uk"):
                return url
        return None  # If all results are filtered out
    except Exception as e:
        print(f"Error searching for {query}: {e}")
        return None

def add_url_col(input_file: str, output_file: str) -> bool:
    """Adds column containing company URL to each record"""

    # checking which filetype to read with
    try:
        if input_file.lower().endswith(".csv"):
            df = pd.read_csv(input_file)
        elif input_file.lower().endswith(".xls"):
            df = pd.read_excel(input_file)
        elif input_file.lower().endswith(".xlsx"):
            df = pd.read_excel(input_file)
        elif input_file.lower().endswith(".xlsm"):
            df = pd.read_excel(input_file)
        else:
            print("Invalid file type for input file")
            return False
    except:
        print("File does not exist")
        return False
    
    # checking output file validity before proceeding
    if output_file.lower().endswith(".csv") or output_file.lower().endswith(".xls") or output_file.lower().endswith(".xlsx") or output_file.lower().endswith(".xlsm"):
        pass
    else:
        print("Output file type not valid")
        return False
    
    # adding new column for business url
    tqdm.pandas(desc="Searching for URLs")
    df["URL"] = df.progress_apply(find_url, args=(5,), axis=1)

    # saving file to output
    if output_file.lower().endswith(".csv"):
        df.to_csv(output_file, index=False)
    else:
        df.to_excel(output_file, index=False)

    print(f"Updated file saved as {output_file}")
    return True

def list_to_string(arr: list) -> str:
    """Helper function for formatting a list as a string nicely"""
    output = ""
    for item in arr:
        output += item
        output += ", "
    output = output[0:-2]
    return output

def scrape_emails(row: pd.Series) -> str:
    """Scrapes email addresses relating to the row given"""
    try:
        url = row["URL"]
    except:
        print("No 'URL' column found - file invalid")
        return
    
    try:
        # Send a GET request to each company page
        response = httpx.get(url=url, timeout=10)
        response.raise_for_status()  # Raise HTTP errors if they occur
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Attempt to extract the company name
        company_name = soup.select_one("h1.dockable.business-name")
        company_name = company_name.text.strip() if company_name else url  # Fallback to URL
        
        # Find email addresses from `mailto:` links
        page_emails = set()  # Use a set to avoid duplicates
        for mailto_link in soup.find_all("a", href=re.compile(r"^mailto:")):
            email = mailto_link.get("href").replace("mailto:", "").strip()
            page_emails.add(email)
        
        # Regex fallback for plain-text emails
        text_emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", soup.get_text())
        page_emails.update(text_emails)

        return list_to_string(list(page_emails))

    except Exception as e:
        print(f"Error processing {url}: {e}")

def add_email_col(input_file: str, output_file: str) -> bool:
    """Adds email column containing emails scraped from URL in URL column"""

    # checking which filetype to read with
    try:
        if input_file.lower().endswith(".csv"):
            df = pd.read_csv(input_file)
        elif input_file.lower().endswith(".xls"):
            df = pd.read_excel(input_file)
        elif input_file.lower().endswith(".xlsx"):
            df = pd.read_excel(input_file)
        elif input_file.lower().endswith(".xlsm"):
            df = pd.read_excel(input_file)
        else:
            print("Invalid file type for input file")
            return False
    except:
        print("File does not exist")
        return False
    
    # checking output file validity before proceeding
    if output_file.lower().endswith(".csv") or output_file.lower().endswith(".xls") or output_file.lower().endswith(".xlsx") or output_file.lower().endswith(".xlsm"):
        pass
    else:
        print("Output file type not valid")
        return False
    
    # progress bar functionality
    tqdm.pandas(desc="Searching for Emails")
    df["emails"] = df.progress_apply(scrape_emails, axis=1)

    # saving file to output
    if output_file.lower().endswith(".csv"):
        df.to_csv(output_file, index=False)
    else:
        df.to_excel(output_file, index=False)

    print(f"Updated file saved as {output_file}")
    return True

def option_1() -> bool:
    """Runs sequence for menu option 1"""
    filename = input("What is the filename? \n")
    postcode = input("What is the origin postcode? \n")

    print("WARNING: This operation will overwrite any current data in a 'distance_miles' column")
    proceed = input("Do you wish to proceed? (y/n) \n")
    if proceed.lower() == "y":
        print("Proceeding")
        return add_dist_col(filename, postcode)
    else:
        print("Aborted")
        return False

def option_2() -> bool:
    """Runs sequence for menu option 2"""
    input_file = input("What is the input filename? \n")
    output_file = input("What is the output filename? \n")

    time.sleep(0.2)
    print("WARNING: URLs found are just estimates and should not be assumed true without checking")
    time.sleep(1)

    print("WARNING: This operation will overwrite any current data in a 'URL' column")
    proceed = input("Do you wish to proceed? (y/n) \n")
    if proceed.lower() == "y":
        print("Proceeding")
        return add_url_col(input_file, output_file)
    else:
        print("Aborted")
        return False

def option_3() -> bool:
    input_file = input("What is the input filename? \n")
    output_file = input("What is the output filename? \n")

    time.sleep(0.2)
    print("WARNING: URLs and emails found are just estimates and should not be assumed true without checking")
    time.sleep(0.2)
    print("WARNING: email addresses may contain formatting errors to be corrected by inspection")

    print("WARNING: This operation will overwrite any current data in 'URL' or 'emails' columns")
    proceed = input("Do you wish to proceed? (y/n) \n")
    if proceed.lower() == "y":
        print("Proceeding")
        url_adding = add_url_col(input_file, output_file)
        if url_adding:
            return add_email_col(output_file, output_file)
        else:
            return False
    else:
        print("Aborted")
        return False
    
def option_4() -> bool:
    input_file = input("What is the input filename? \n")
    output_file = input("What is the output filename? \n")

    time.sleep(0.5)
    print("WARNING: emails found are just estimates and should not be assumed true without checking")
    time.sleep(0.5)
    print("WARNING: email addresses may contain formatting errors to be corrected by inspection")
    time.sleep(0.5)
    print("WARNING: This operation will overwrite any current data in 'emails' column ")

    proceed = input("Do you wish to proceed? (y/n) \n")
    if proceed.lower() == "y":
        print("Proceeding")
        return add_email_col(input_file, output_file)
    else:
        print("Aborted")
        return False

def print_menu() -> None:
    print("### Companies House Web Scraping Tool ###")
    time.sleep(0.5)
    print("Menu: \n1. Add distance column to sheet \n2. Web scrape for company URLs only \n3. Web scrape for both company URLs and Email addresses \n4. Web scrape for email addresses only (requires option 2 doing first) \nQ. Quit")
    time.sleep(0.5)

if __name__=="__main__":
    while True:
        print_menu()
        choice = input("Type '1', '2', '3', or 'Q' \n")
        if choice.lower() == "q":
            print("Program quit selected")
            sys.exit(0)
        elif choice == "1":
            success = option_1()
            if success:
                print("SUCCESS: Distance column added")
        elif choice == "2":
            success = option_2()
            if success:
                print("SUCCESS: URL column added")
        elif choice == "3":
            success = option_3()
            if success:
                print("SUCCESS: URL and Email columns added")
        elif choice == "4":
            success = option_4()
            if success:
                print("SUCCESS: Email columns added")
        else:
            print("Invalid option")
            time.sleep(0.2)
            print("Please try again")