from datetime import datetime
from time import sleep
import os

from bs4 import BeautifulSoup
from requests import get
import pandas as pd

base_keywords = [
    "аналитик данных",
    "data analyst",
    "продуктовый аналитик",
    "bi аналитик",
    "системный аналитик",
    "аналитик",
]

town_ids = [4, 14]


def load_env(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ[key] = value


def request_api(
    url: str, api_key: str, params: dict | None = None
) -> dict | list | None:
    response = get(
        url,
        headers={"X-Api-App-Id": api_key},
        params=params,
        timeout=20,
    )
    if response.status_code != 200:
        print("error", response.status_code, url, params)
        return None
    return response.json()


def get_salary(vacancy: dict) -> str | None:
    payment_from = vacancy.get("payment_from") or 0
    payment_to = vacancy.get("payment_to") or 0
    currency = vacancy.get("currency")

    if payment_from and payment_to:
        return f"от {payment_from} до {payment_to} {currency}"
    if payment_from:
        return f"от {payment_from} {currency}"
    if payment_to:
        return f"до {payment_to} {currency}"
    return None


def get_skills(text: str | None) -> list[str]:
    if text is None:
        return []

    foundSkills = []
    for skill in [
        "Python",
        "SQL",
        "Pandas",
        "NumPy",
        "SciPy",
        "Excel",
        "Power BI",
        "Tableau",
        "ClickHouse",
        "PostgreSQL",
        "Airflow",
        "Spark",
        "Kafka",
        "Docker",
        "Git",
        "A/B тесты",
    ]:
        if skill.lower() in text.lower():
            foundSkills.append(skill)
    return foundSkills


def get_description(vacancy: dict) -> str | None:
    if vacancy.get("candidat"):
        return vacancy["candidat"].strip().replace("\n", "\\n")
    if vacancy.get("vacancyRichText"):
        return (
            BeautifulSoup(vacancy["vacancyRichText"], "html.parser")
            .get_text("\n", strip=True)
            .replace("\n", "\\n")
        )
    return None


def get_towns(api_key: str) -> list[dict]:
    data = request_api(
        "https://api.superjob.ru/2.0/towns/",
        api_key,
        params={"all": 1},
    )
    if data is None:
        return []
    print("towns", len(data.get("objects", [])))
    return data.get("objects", [])


def towns_to_rows(towns: list[dict]) -> list[dict]:
    rows = []
    for town in towns:
        rows.append(
            {
                "id": town.get("id"),
                "id_region": town.get("id_region"),
                "id_country": town.get("id_country"),
                "title": town.get("title"),
                "title_eng": town.get("title_eng"),
            }
        )
    return rows


def get_regions_combined(api_key: str) -> list[dict]:
    data = request_api("https://api.superjob.ru/2.0/regions/combined/", api_key)
    if data is None:
        return []
    return data


def regions_to_rows(countries: list[dict]) -> list[dict]:
    rows = []
    for country in countries:
        country_id = country.get("id")
        country_title = country.get("title")

        for town in country.get("towns", []):
            rows.append(
                {
                    "country_id": country_id,
                    "country_title": country_title,
                    "region_id": None,
                    "region_title": None,
                    "town_id": town.get("id"),
                    "town_title": town.get("title"),
                }
            )

        for region in country.get("regions", []):
            for town in region.get("towns", []):
                rows.append(
                    {
                        "country_id": country_id,
                        "country_title": country_title,
                        "region_id": region.get("id"),
                        "region_title": region.get("title"),
                        "town_id": town.get("id"),
                        "town_title": town.get("title"),
                    }
                )
    return rows


def get_catalogues(api_key: str) -> list[dict]:
    data = request_api("https://api.superjob.ru/2.0/catalogues/", api_key)
    if data is None:
        return []
    return data


def catalogues_to_rows(catalogues: list[dict]) -> list[dict]:
    rows = []
    for catalogue in catalogues:
        rows.append(
            {
                "catalogue_id": catalogue.get("key"),
                "catalogue_title": catalogue.get("title"),
                "position_id": None,
                "position_title": None,
            }
        )
        for position in catalogue.get("positions", []):
            rows.append(
                {
                    "catalogue_id": catalogue.get("key"),
                    "catalogue_title": catalogue.get("title"),
                    "position_id": position.get("key"),
                    "position_title": position.get("title"),
                }
            )
    return rows


def get_metro_lines(town_id: int, api_key: str) -> list[dict]:
    data = request_api(f"https://api.superjob.ru/2.0/metro/{town_id}/lines/", api_key)
    if data is None:
        return []
    return data


def metro_to_rows(lines: list[dict], town_id: int) -> list[dict]:
    rows = []
    for line in lines:
        for station in line.get("stations", []):
            rows.append(
                {
                    "town_id": town_id,
                    "line_id": line.get("id"),
                    "line_title": line.get("title"),
                    "line_color": line.get("color"),
                    "station_id": station.get("id"),
                    "station_title": station.get("title"),
                }
            )
    return rows


def get_vacancies(
    keyword: str, town_id: int, api_key: str, page: int = 0, count: int = 100
) -> dict | None:
    data = request_api(
        "https://api.superjob.ru/2.0/vacancies/",
        api_key,
        params={
            "keyword": keyword,
            "town": town_id,
            "page": page,
            "count": count,
        },
    )
    if data is None:
        return None
    return data


def vacancy_to_row(vacancy: dict, keyword: str) -> dict:
    description = get_description(vacancy)
    client = vacancy.get("client") or {}
    catalogues = vacancy.get("catalogues") or []
    return {
        "id": vacancy.get("id"),
        "url": vacancy.get("link"),
        "name": vacancy.get("profession"),
        "salary": get_salary(vacancy),
        "salary_from": vacancy.get("payment_from") or None,
        "salary_to": vacancy.get("payment_to") or None,
        "currency": vacancy.get("currency"),
        "description": description,
        "skills": get_skills(description),
        "company": vacancy.get("firm_name") or client.get("title"),
        "address": vacancy.get("address") or client.get("address"),
        "area": (vacancy.get("town") or {}).get("title"),
        "experience": (vacancy.get("experience") or {}).get("title"),
        "employment": (vacancy.get("type_of_work") or {}).get("title"),
        "schedule": (vacancy.get("type_of_work") or {}).get("title"),
        "working_hours": None,
        "work_format": (vacancy.get("place_of_work") or {}).get("title"),
        "published_at": (
            None
            if vacancy.get("date_published") is None
            else datetime.fromtimestamp(vacancy["date_published"]).isoformat()
        ),
        "catalogue_ids": [catalogue.get("key") for catalogue in catalogues],
        "catalogue_titles": [catalogue.get("title") for catalogue in catalogues],
        "source": "superjob",
        "query": keyword,
    }


def get_all_vacancies(
    keywords: list[str], towns: list[int], api_key: str, max_pages: int = 5
) -> list[dict]:
    data = []
    seen_ids = set()

    for keyword in keywords:
        for town_id in towns:
            page = 0
            while page < max_pages:
                vacancies = get_vacancies(keyword, town_id, api_key, page=page)
                if vacancies is None:
                    break

                objects = vacancies.get("objects", [])
                print("vacancies", keyword, town_id, page, len(objects))
                for vacancy in objects:
                    vacancy_id = vacancy.get("id")
                    if vacancy_id in seen_ids:
                        continue
                    seen_ids.add(vacancy_id)
                    data.append(vacancy_to_row(vacancy, keyword))

                if not vacancies.get("more"):
                    break

                page += 1
                sleep(1)
    return data


def get_vacancy_by_id(vacancy_id: int, api_key: str) -> dict | None:
    data = request_api(f"https://api.superjob.ru/2.0/vacancies/{vacancy_id}/", api_key)
    if data is None:
        return None
    return data


def vacancy_detail_to_row(vacancy: dict) -> dict:
    description = get_description(vacancy)
    client = vacancy.get("client") or {}
    phones = vacancy.get("phones") or []
    catalogues = vacancy.get("catalogues") or []
    metro = vacancy.get("metro") or []

    return {
        "id": vacancy.get("id"),
        "url": vacancy.get("link"),
        "name": vacancy.get("profession"),
        "salary": get_salary(vacancy),
        "salary_from": vacancy.get("payment_from") or None,
        "salary_to": vacancy.get("payment_to") or None,
        "currency": vacancy.get("currency"),
        "description": description,
        "skills": get_skills(description),
        "company": vacancy.get("firm_name") or client.get("title"),
        "company_url": client.get("url"),
        "company_description": client.get("description"),
        "address": vacancy.get("address") or client.get("address"),
        "area": (vacancy.get("town") or {}).get("title"),
        "experience": (vacancy.get("experience") or {}).get("title"),
        "employment": (vacancy.get("type_of_work") or {}).get("title"),
        "work_format": (vacancy.get("place_of_work") or {}).get("title"),
        "education": (vacancy.get("education") or {}).get("title"),
        "phones": phones,
        "phone_count": len(phones),
        "metro": metro,
        "metro_count": len(metro),
        "catalogue_ids": [catalogue.get("key") for catalogue in catalogues],
        "catalogue_titles": [catalogue.get("title") for catalogue in catalogues],
        "is_closed": vacancy.get("is_closed"),
        "is_archive": vacancy.get("is_archive"),
        "date_published": (
            None
            if vacancy.get("date_published") is None
            else datetime.fromtimestamp(vacancy["date_published"]).isoformat()
        ),
        "date_pub_to": (
            None
            if vacancy.get("date_pub_to") is None
            else datetime.fromtimestamp(vacancy["date_pub_to"]).isoformat()
        ),
        "latitude": vacancy.get("latitude"),
        "longitude": vacancy.get("longitude"),
        "id_client": vacancy.get("id_client"),
        "source": "superjob_detail",
    }


def get_all_vacancy_details(vacancy_ids: list[int], api_key: str) -> list[dict]:
    rows = []
    for i, vacancy_id in enumerate(vacancy_ids):
        print("vacancy_detail", i + 1, "/", len(vacancy_ids), vacancy_id)
        vacancy = get_vacancy_by_id(vacancy_id, api_key)
        if vacancy is None:
            continue
        rows.append(vacancy_detail_to_row(vacancy))
        sleep(0.3)
    return rows


if __name__ == "__main__":
    load_env()
    api_key = os.getenv("SUPERJOB_TOKEN") or os.getenv("SUPERJOB_ID")
    if api_key is None:
        raise RuntimeError("SUPERJOB_TOKEN not found in .env")

    towns = get_towns(api_key)
    towns_df = pd.DataFrame(towns_to_rows(towns))
    towns_df.to_csv("./data/superjob_towns.csv")

    regions = get_regions_combined(api_key)
    regions_df = pd.DataFrame(regions_to_rows(regions))
    regions_df.to_csv("./data/superjob_regions.csv")

    catalogues = get_catalogues(api_key)
    catalogues_df = pd.DataFrame(catalogues_to_rows(catalogues))
    catalogues_df.to_csv("./data/superjob_catalogues.csv")

    metro_rows = []
    for town_id in town_ids:
        print("metro_lines", town_id)
        metro_rows.extend(metro_to_rows(get_metro_lines(town_id, api_key), town_id))
    metro_df = pd.DataFrame(metro_rows)
    metro_df.to_csv("./data/superjob_metro.csv")

    vacancies = get_all_vacancies(base_keywords, town_ids, api_key)
    vacancies_df = pd.DataFrame(vacancies)
    vacancies_df.to_csv("./data/superjob.csv")

    vacancy_ids = vacancies_df["id"].dropna().astype(int).tolist()
    vacancy_details = get_all_vacancy_details(vacancy_ids, api_key)
    vacancy_details_df = pd.DataFrame(vacancy_details)
    vacancy_details_df.to_csv("./data/superjob_details.csv")
