import json
import logging
import os
import re
from time import sleep
from urllib.parse import quote_plus

import pandas as pd
import ua_generator
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.safari.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

try:
    from logging_settings import setup_logging
except ImportError:
    from scraper.logging_settings import setup_logging

base_keywords = [
    "аналитик данных",
    "data analyst",
    "data scientist",
    "python analyst",
]

max_pages_per_query = 3
save_every = 32
sleep_after_get = 1.5
out_csv_path = "./data/hh_selenium.csv"

setup_logging()
logger = logging.getLogger(__name__)


def get_text(obj) -> str | None:
    if obj is None:
        return None
    return obj.get_text(" ", strip=True)


def get_ld_json(page: BeautifulSoup) -> dict:
    jsonObj = page.find("script", attrs={"type": "application/ld+json"})
    if jsonObj is None:
        return {}
    try:
        return json.loads(jsonObj.text.strip())
    except Exception:
        return {}


def get_description(page: BeautifulSoup, ld_json: dict) -> str | None:
    if ld_json.get("description"):
        return (
            BeautifulSoup(ld_json["description"], "html.parser")
            .get_text("\n", strip=True)
            .replace("\n", "\\n")
        )

    descriptionObj = page.find("div", attrs={"data-qa": "vacancy-description"})
    description = get_text(descriptionObj)
    if description is None:
        return None
    return description.replace("\n", "\\n")


def get_salary(page: BeautifulSoup) -> str | None:
    salaryObj = page.find(
        "span", attrs={"data-qa": "vacancy-salary-compensation-type-net"}
    )
    if salaryObj is None:
        salaryObj = page.find(
            "span", attrs={"data-qa": "vacancy-salary-compensation-type-gross"}
        )
    if salaryObj is None:
        salaryObj = page.find("div", attrs={"data-qa": "vacancy-salary"})
    if salaryObj is None:
        salaryObj = page.find("span", attrs={"data-qa": "compensation-value"})
    if salaryObj is None:
        return None
    return salaryObj.get_text(" ", strip=True).replace("\xa0", " ")


def get_skills(page: BeautifulSoup, text: str | None) -> list[str]:
    skillsObj = page.find_all("li", attrs={"data-qa": "skills-element"})
    if skillsObj:
        return list(map(lambda x: x.text.strip(), skillsObj))

    foundSkills = []
    if text is None:
        return foundSkills

    for skill in [
        "Python",
        "SQL",
        "Pandas",
        "NumPy",
        "Excel",
        "Power BI",
        "Tableau",
        "ClickHouse",
        "PostgreSQL",
        "Airflow",
        "Spark",
        "Docker",
        "Git",
    ]:
        if skill.lower() in text.lower():
            foundSkills.append(skill)
    return foundSkills


def get_existing_data(path: str) -> tuple[list[dict], set[int]]:
    if not os.path.exists(path):
        return [], set()

    oldDf = pd.read_csv(path)
    unnamedCols = list(filter(lambda x: x.startswith("Unnamed:"), oldDf.columns))
    if unnamedCols:
        oldDf = oldDf.drop(columns=unnamedCols)

    existing_ids = set()
    if "id" in oldDf.columns:
        existing_ids = set(
            pd.to_numeric(oldDf["id"], errors="coerce").dropna().astype(int)
        )

    return oldDf.to_dict("records"), existing_ids


def build_search_url(keyword: str, page: int = 0) -> str:
    return f"https://hh.ru/search/vacancy?text={quote_plus(keyword)}&page={page}"


def make_driver(headless: bool = False) -> webdriver.Safari:
    options = Options()

    # В Safari это не всегда умеется, но если вдруг умеется, то почему бы и нет
    if headless:
        try:
            options.add_argument("--headless")
        except Exception:
            logger.info("headless for safari looks suspicious, ignoring")

    driver = webdriver.Safari(options=options)
    driver.set_page_load_timeout(60)
    return driver


def wait_page_a_bit(driver: webdriver.Safari, seconds: float = sleep_after_get) -> None:
    sleep(seconds)
    try:
        WebDriverWait(driver, 10).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except Exception:
        pass


def close_region_popup_if_any(driver: webdriver.Safari) -> None:
    for selector in [
        '[data-qa="bloko-modal-close"]',
        '[data-qa="relocation-warning-confirm"]',
        '[data-qa="relocation-warning-cancel"]',
    ]:
        try:
            button = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
            )
            button.click()
            sleep(0.5)
            return
        except Exception:
            pass


def get_vacancies_ids(
    driver: webdriver.Safari,
    keyword: str,
    page: int = 0,
) -> list[int] | None:
    url = build_search_url(keyword, page)
    logger.info("search request url=%s", url)

    try:
        driver.get(url)
    except TimeoutException:
        logger.warning("search timeout url=%s", url)
        return None
    except WebDriverException as e:
        logger.warning("search webdriver error url=%s error=%s", url, e)
        return None

    wait_page_a_bit(driver)
    close_region_popup_if_any(driver)

    if "Подтвердите, что вы не робот" in driver.page_source:
        logger.warning("search captcha url=%s", url)
        return None

    pageSoup = BeautifulSoup(driver.page_source, "html.parser")
    ids = []

    for el in pageSoup.find_all("a", href=re.compile(r"/vacancy/\d+")):
        href = el.get("href") or ""
        idMatch = re.search(r"/vacancy/(\d+)", href)
        if idMatch is None:
            continue
        ids.append(int(idMatch.group(1)))

    ids = list(dict.fromkeys(ids))
    return ids


def get_all_vacancies_ids(
    driver: webdriver.Safari,
    keywords: list[str],
    max_pages: int = max_pages_per_query,
) -> list[tuple[int, str]]:
    allIDs = []
    for keyword in keywords:
        for i in range(max_pages):
            ids = get_vacancies_ids(driver, keyword, page=i)
            if ids is None or len(ids) == 0:
                break
            logger.info(
                "search page parsed keyword=%s page=%s count=%s",
                keyword,
                i,
                len(ids),
            )
            for id in ids:
                allIDs.append((id, keyword))
    return list(dict.fromkeys(allIDs))


def get_vacancy_by_id(
    driver: webdriver.Safari,
    id: int,
    query: str | None = None,
) -> dict | None:
    url = f"https://hh.ru/vacancy/{id}"
    logger.info("vacancy request url=%s", url)

    try:
        driver.get(url)
    except TimeoutException:
        logger.warning("vacancy timeout url=%s", url)
        return None
    except WebDriverException as e:
        logger.warning("vacancy webdriver error url=%s error=%s", url, e)
        return None

    wait_page_a_bit(driver)

    if "Подтвердите, что вы не робот" in driver.page_source:
        logger.warning("vacancy captcha url=%s", url)
        return None

    page = BeautifulSoup(driver.page_source, "html.parser")
    ld_json = get_ld_json(page)

    titleObj = page.find(attrs={"data-qa": "vacancy-title"})
    companyObj = page.find(attrs={"data-qa": "vacancy-company-name"})
    areaObj = page.find(attrs={"data-qa": "vacancy-view-location"})
    publishedObj = page.find(
        "p", attrs={"class": re.compile(r".*vacancy-creation-time-redesigned.*")}
    )

    description = get_description(page, ld_json)
    textForSkills = driver.page_source + "\n" + (description or "")

    return {
        "id": id,
        "url": driver.current_url,
        "name": ld_json.get("title") or get_text(titleObj),
        "salary": get_salary(page),
        "description": description,
        "skills": get_skills(page, textForSkills),
        "company": (
            ld_json.get("hiringOrganization", {}).get("name") or get_text(companyObj)
        ),
        "area": (
            ((ld_json.get("jobLocation") or {}).get("address") or {}).get(
                "addressLocality"
            )
            or get_text(areaObj)
        ),
        "published_at": ld_json.get("datePosted") or get_text(publishedObj),
        "source": "hh",
        "query": query,
    }


if __name__ == "__main__":
    oldData, existing_ids = get_existing_data(out_csv_path)
    logger.info("already saved %s", len(existing_ids))

    # Если есть одинокий ассист который пытается это гонять:
    # Надо менять настройки safari -
    # Develop -> Allow Remote Automation. Прекрасный UX, просто а*****ый.
    driver = make_driver()
    data = oldData[:]

    try:
        idsAndQueries = get_all_vacancies_ids(driver, base_keywords)
        idsAndQueries = list(filter(lambda x: x[0] not in existing_ids, idsAndQueries))
        logger.info("need to parse %s", len(idsAndQueries))

        for i, (vacancy_id, query) in enumerate(idsAndQueries, start=1):
            vacancy = get_vacancy_by_id(driver, vacancy_id, query=query)
            if vacancy is not None:
                data.append(vacancy)

            if i % save_every == 0:
                logger.info(
                    "saving %s from %s parsed %s", i, len(idsAndQueries), len(data)
                )
                df = pd.DataFrame(data)
                df.to_csv(out_csv_path)

        logger.info("parsed vacancies %s from %s", len(data), len(idsAndQueries))
        df = pd.DataFrame(data)
        df.to_csv(out_csv_path)
    finally:
        driver.quit()
