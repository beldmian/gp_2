import json
import logging
import os
import re
from random import choice, shuffle
import multiprocessing
from itertools import batched
from time import sleep

import ua_generator
from bs4 import BeautifulSoup
from requests import get
import pandas as pd

# Очень не хочется думать над путем импорта, не бейте пжпж
try:
    from logging_settings import setup_logging
except ImportError:
    from scraper.logging_settings import setup_logging

base_urls = [
    "https://hh.ru/search/vacancy?area=1&professional_role=156",
    "https://hh.ru/search/vacancy?area=1&professional_role=10",
    "https://hh.ru/search/vacancy?area=1&professional_role=164",
    "https://hh.ru/search/vacancy?area=1&professional_role=157",
    "https://hh.ru/vacancies/analitik-dannyh",
    "https://hh.ru/vacancies/data-scientist",
    "https://hh.ru/vacancies/data-analyst",
    "https://hh.ru/vacancies/analitik",
    "https://hh.ru/vacancies/analitik-biznes-protsessov",
    "https://hh.ru/vacancies/data-engineer",
    # Не поддерживается, я пытался
    # "https://hh.ru/search/vacancy?text=bi+analyst",
    # "https://hh.ru/search/vacancy?text=product+analyst",
    # "https://hh.ru/search/vacancy?text=data+engineer",
    # "https://hh.ru/search/vacancy?text=ml+engineer",
]

save_every = 64
out_csv_path = "./data/hh.csv"

setup_logging()
logger = logging.getLogger(__name__)


def repeat_to_length(lst, target_length):
    if not lst:
        return []
    q, r = divmod(target_length, len(lst))
    return lst * q + lst[:r]


def get_text(obj) -> str | None:
    if obj is None:
        return None
    return obj.get_text(" ", strip=True)


def clean_label(text: str | None, label: str) -> str | None:
    if text is None:
        return None
    if text.startswith(label):
        return text[len(label) :].strip()
    return text


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
    return get_text(descriptionObj).replace("\n", "\\n")


def get_salary(page: BeautifulSoup) -> str | None:
    salaryObj = page.find("div", attrs={"data-qa": "vacancy-salary"})
    if salaryObj is not None:
        return salaryObj.text.strip().replace("\xa0", " ")

    metaDescriptionObj = page.find("meta", attrs={"name": "description"})
    if metaDescriptionObj is not None and metaDescriptionObj.has_attr("content"):
        salaryMatch = re.search(
            r"Зарплата:\s*(.*?)(?:\.|$)", metaDescriptionObj["content"]
        )
        if salaryMatch is not None:
            salary = salaryMatch.group(1).strip()
            if salary != "не указана":
                return salary
    return None


def get_skills(page: BeautifulSoup, text: str) -> list[str]:
    skillsObj = page.find_all("li", attrs={"data-qa": "skills-element"})
    if skillsObj:
        return list(map(lambda x: x.text.strip(), skillsObj))

    skillsMatch = re.search(r'"keySkill":(\[[^\]]*\])', text)
    if skillsMatch is not None:
        try:
            return json.loads(skillsMatch.group(1))
        except Exception:
            pass

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


def _check_proxy(proxy: str) -> str | None:
    resp = get(
        "https://hh.ru",
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) Gecko/20100101 Firefox/148.0"
        },
        proxies={"http": f"socks5://{proxy}"},
        timeout=10,
    )
    logger.info("checked proxy %s %s", proxy, resp.status_code)
    if resp.status_code == 200:
        return proxy
    return None


def get_proxies_list(
    raw_proxies: list[str], n: int = 32, workers: int | None = None
) -> list[str]:
    raw_proxies = raw_proxies[:]
    shuffle(raw_proxies)

    with multiprocessing.Pool(processes=16) as pool:
        results = pool.imap_unordered(_check_proxy, raw_proxies, chunksize=1)

        out_proxies = []
        for proxy in results:
            if proxy is not None:
                out_proxies.append(proxy)
                if len(out_proxies) >= n:
                    pool.terminate()
                    pool.join()
                    break
    return out_proxies


def get_vacancies_ids(
    url: str,
    proxies: list[str],
    page: int = 0,
    retries: int = 3,
) -> list[int] | None:
    pageParam = "&" if "?" in url else "?"
    try:
        response = get(
            f"{url}{pageParam}page={page}",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:148.0) Gecko/20100101 Firefox/148.0"
            },
            proxies={
                "http": f"socks5://{choice(proxies)}",
            },
        )
    except Exception as e:
        logger.warning(
            "vacancies ids request error url=%s page=%s retries=%s error=%s",
            url,
            page,
            retries,
            e,
        )
        if retries > 0:
            return get_vacancies_ids(url, proxies, page, retries - 1)
        return None
    if response.status_code != 200:
        logger.warning(
            "vacancies ids bad status url=%s page=%s status=%s retries=%s",
            url,
            page,
            response.status_code,
            retries,
        )
        if retries > 0:
            return get_vacancies_ids(url, proxies, page, retries - 1)
        return None
    elif "Подтвердите, что вы не робот" in response.text:
        logger.warning(
            "vacancies ids captcha url=%s page=%s retries=%s proxies=%s",
            url,
            page,
            retries,
            proxies,
        )
        if retries > 0:
            return get_vacancies_ids(url, proxies, page, retries - 1)
    page = BeautifulSoup(response.text)

    ids = []
    for el in page.find_all("div", attrs={"class": re.compile(r".*vacancy-card.*")}):
        if not el.has_attr("id"):
            continue
        if not el.attrs["id"].isdigit():
            continue
        ids.append(el.attrs["id"])
    return ids


def get_all_vacancies_ids(urls: list[str], proxies: list[str]) -> list[int]:
    allIDs = []
    for url in urls:
        i = 0
        while True:
            ids = get_vacancies_ids(url, proxies, page=i)
            if ids is None or len(ids) == 0:
                break
            logger.info(
                "vacancies page parsed url=%s page=%s count=%s", url, i, len(ids)
            )
            i += 1
            allIDs.extend(ids)
    return allIDs


def get_vacancy_by_id(id: int, proxies: list[str], retries: int = 3) -> dict | None:
    logger.info(
        "vacancy request url=%s proxies=%s", f"https://hh.ru/vacancy/{id}", proxies
    )
    try:
        response = get(
            f"https://hh.ru/vacancy/{id}",
            headers=ua_generator.generate(device="desktop").headers.get(),
            proxies={
                "http": f"socks5://{choice(proxies)}",
            },
        )
    except Exception as e:
        logger.warning(
            "vacancy request error url=%s retries=%s error=%s",
            f"https://hh.ru/vacancy/{id}",
            retries,
            e,
        )
        return None
    if response.status_code != 200:
        logger.warning(
            "vacancy bad status url=%s status=%s proxies=%s",
            f"https://hh.ru/vacancy/{id}",
            response.status_code,
            proxies,
        )
        return None
    if "Подтвердите, что вы не робот" in response.text:
        logger.warning(
            "vacancy captcha url=%s retries=%s proxies=%s",
            f"https://hh.ru/vacancy/{id}",
            retries,
            proxies,
        )
        if retries > 0:
            sleep(30)
            return get_vacancy_by_id(id, proxies, retries=retries - 1)
        else:
            return None
    page = BeautifulSoup(response.text, "html.parser")
    ld_json = get_ld_json(page)

    titleObj = page.find(attrs={"data-qa": "vacancy-title"})
    companyObj = page.find(attrs={"data-qa": "vacancy-company-name"})
    addressObj = page.find("span", attrs={"data-qa": "vacancy-view-raw-address"})

    area = None
    address = get_text(addressObj)
    if ld_json.get("jobLocation") is not None:
        addressObjJson = ld_json["jobLocation"].get("address", {})
        area = addressObjJson.get("addressLocality")
        if address is None:
            addressParts = [
                addressObjJson.get("streetAddress"),
                addressObjJson.get("addressLocality"),
                addressObjJson.get("addressRegion"),
            ]
            address = ", ".join(filter(None, addressParts)) or None

    description = get_description(page, ld_json)
    experience = get_text(page.find(attrs={"data-qa": "vacancy-experience"}))
    employment = get_text(page.find(attrs={"data-qa": "common-employment-text"}))
    schedule = clean_label(
        get_text(page.find(attrs={"data-qa": "work-schedule-by-days-text"})),
        "График:",
    )
    working_hours = clean_label(
        get_text(page.find(attrs={"data-qa": "working-hours-text"})),
        "Рабочие часы:",
    )
    work_format = clean_label(
        get_text(page.find(attrs={"data-qa": "work-formats-text"})),
        "Формат работы:",
    )

    return {
        "id": id,
        "url": response.url,
        "name": ld_json.get("title") or get_text(titleObj),
        "salary": get_salary(page),
        "description": description,
        "skills": get_skills(page, response.text + "\n" + (description or "")),
        "company": (
            ld_json.get("hiringOrganization", {}).get("name") or get_text(companyObj)
        ),
        "address": address,
        "area": area,
        "experience": experience,
        "employment": employment,
        "schedule": schedule,
        "working_hours": working_hours,
        "work_format": work_format,
        "published_at": ld_json.get("datePosted"),
        "archived": page.find(attrs={"data-qa": "vacancy-title-archived-text"})
        is not None,
        "source": "hh",
    }


def get_vacancy_by_id_task(args) -> dict | None:
    return get_vacancy_by_id(args[0], args[1])


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


if __name__ == "__main__":
    socks = []
    with open("misc/socks5.txt", "r") as socks_file:
        socks = socks_file.read().splitlines()

    # proxies = get_proxies_list(socks)
    proxies = socks
    pool = multiprocessing.Pool(32)

    oldData, existing_ids = get_existing_data(out_csv_path)
    logger.info("already saved %s", len(existing_ids))

    ids = get_all_vacancies_ids(base_urls, proxies)
    ids = list(filter(lambda x: x not in existing_ids, ids))
    logger.info("need to parse %s", len(ids))

    proxiesLists = repeat_to_length(list(batched(proxies, 1)), len(ids))
    argsList = list(zip(ids, proxiesLists))
    data = oldData[:]
    for i, vacancy in enumerate(
        pool.imap_unordered(get_vacancy_by_id_task, argsList, chunksize=1), start=1
    ):
        if vacancy is not None:
            data.append(vacancy)

        if i % save_every == 0:
            logger.info("saving %s from %s parsed %s", i, len(ids), len(data))
            df = pd.DataFrame(data)
            df.to_csv(out_csv_path)

    logger.info("parsed vacancies %s from %s", len(data), len(ids))
    df = pd.DataFrame(data)
    df.to_csv(out_csv_path)
