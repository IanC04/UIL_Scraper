import requests
from bs4 import BeautifulSoup
import csv
from tqdm import tqdm
import concurrent.futures
import sys

HOME_URL = f"https://utdirect.utexas.edu/nlogon/uil/vlcp_pub_arch.WBX"
EVENTS = ["ACC", "BJE", "CAL", "COM", "CSC", "CON", "CPY", "CXD", "CIE", "EWR", "FWR", "CAN",
          "DOC", "NAR", "TAN", "HWR", "INF", "JRN", "LHE", "LDO", "LIT", "MTH", "NWR", "NUM",
          "OAP", "PER", "POE", "PRO", "RWR", "RBB", "RBF", "RFF", "SCI", "SOC", "SPE", "SPV",
          "THT", "COS", "GRP", "HMK", "MKT", "SCN"]


def brackets(level: str) -> list[str]:
    if level == "District":
        return [f"{i}" for i in range(1, 33)]
    if level == "Region":
        return [f"{i}" for i in range(1, 5)]
    if level == "State":
        return [""]


def get_yearly_data(year: int) -> tuple[int, list[list[str]], list[list[str]]]:
    print(f"Parsing {year}")
    ind_data = []
    team_data = []
    for conf in tqdm([f"{i}A" for i in range(1, 7)]):
        for level in ["District", "Region", "State"]:
            tiers = brackets(level)
            for nbr in tiers:
                for event in EVENTS:
                    url = (f"https://utdirect.utexas.edu/nlogon/uil/vlcp_pub_arch.WBX?s_year={
                    year}"
                           f"&s_conference={conf}"
                           f"&s_level_id={level[0]}&s_level_nbr={nbr}&s_event_abbr="
                           f"{event}&s_submit_sw=X&s_year=2024"
                           f"&s_conference=4A"
                           f"&s_level_id=D&s_level_nbr=&s_gender=&s_round=&s_dept=C&s_area_zone=")
                    r = requests.get(url)
                    soup = BeautifulSoup(r.content, 'html.parser')
                    tables = soup.find_all('table')
                    if tables is None:
                        print(f"Skipping {year} {conf} {level} {nbr} {event}")
                    else:
                        for table in tables:
                            print(f"Parsing {year} {conf} {level} {nbr} {event}")
                            rows = table.find_all('tr')
                            individual = rows[0].text.strip().startswith("Contestant")
                            # Skip header
                            rows = rows[1:]
                            for row in rows:
                                cells = row.find_all('td')
                                data = [cell.text.strip() for cell in cells]

                                # No advance column for state individual
                                if individual and len(data) < 7:
                                    data.append("")
                                data.append(year)
                                data.append(conf)
                                data.append(level)
                                data.append(nbr)
                                data.append(event)
                                if individual:
                                    ind_data.append(data)
                                else:
                                    team_data.append(data)
    return year, ind_data, team_data


def scrape_data() -> None:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(get_yearly_data, range(2004, 2023)))

    IND_HEADER = ["Contestant", "School", "Score", "Place", "Points", "Medal", "Advance"]
    TEAM_HEADER = ["School", "Team", "Score", "Place", "Points", "Medal", "Advance"]

    for year, ind_data, team_data in results:
        with open(f'Data/INDIVIDUAL_{year}.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(IND_HEADER + ["Year", "Conference", "Level", "Bracket", "Event"])
            for data in ind_data:
                writer.writerow(data)
        with open(f'Data/TEAM_{year}.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(TEAM_HEADER + ["Year", "Conference", "Level", "Bracket", "Event"])
            for data in team_data:
                writer.writerow(data)


if __name__ == "__main__":
    if (len(sys.argv) > 1 and sys.argv[1] == "scrape") or input(
            "Scrape data? (y/n): ").lower() == "y":
        scrape_data()
