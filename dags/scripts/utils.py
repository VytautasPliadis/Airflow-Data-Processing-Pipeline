import logging
from dataclasses import dataclass, asdict
from typing import Union, List, Dict, Any
import requests
from yarl import URL
import feedparser
import csv
import re

logger = logging.getLogger(__name__)


@dataclass
class Job:
    timestamp: str
    title: str = 'Unknown'
    company: str = 'Unknown'
    link: str = 'Unknown'
    job_type: str = 'Unknown'
    region: str = 'Unknown'
    salary: Union[int, str, None] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        return cls(**data)


def split_title_and_company(title: str) -> (str, str):
    parts = title.split(': ', 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return 'Unknown Company', title


def fetch_rss(rss_url: str, job_role: str) -> Union[List[Dict[str, Any]], None]:
    """
    Fetch jobs from an RSS feed.
    """
    try:
        feed = feedparser.parse(rss_url)
        jobs_list = []

        for entry in feed.entries:
            if job_role.lower() in entry.title.lower():
                company, job_title = split_title_and_company(entry.get('title', 'Unknown'))
                job = Job(
                    timestamp=entry.get('published', 'Unknown'),
                    title=job_title,
                    company=company,
                    link=entry.get('link', 'Unknown'),
                    job_type=entry.get('type', 'Unknown'),
                    region=entry.get('region', 'Unknown'),
                    salary=entry.get('salary')
                )
                jobs_list.append(job)

        return [job.to_dict() for job in jobs_list]
    except Exception as e:
        logger.error("An error occurred while fetching RSS feed", exc_info=True)
        return None


def fetch_api(api_url: str, job_role: str) -> Union[List[Dict[str, Any]], None]:
    """
    Fetch jobs from an API.
    """
    url = URL(api_url).with_query(search=job_role)
    try:
        with requests.Session() as session:
            response = session.get(str(url))
            response.raise_for_status()
            data = response.json()

        jobs_data = data.get("jobs", [])
        jobs_list = []

        for job_data in jobs_data:
            if job_role.lower() in job_data['title'].lower():
                job = Job(
                    timestamp=job_data.get('publication_date', 'Unknown'),
                    title=job_data.get('title', 'Unknown'),
                    company=job_data.get('company_name', 'Unknown'),
                    link=job_data.get('url', 'Unknown'),
                    job_type=job_data.get('job_type', 'Unknown'),
                    region=job_data.get('candidate_required_location', 'Unknown'),
                    salary=job_data.get('salary') if job_data.get('salary') != '' else None
                )
                jobs_list.append(job)

        return [job.to_dict() for job in jobs_list]
    except requests.exceptions.RequestException as e:
        logger.error("An error occurred while fetching API data", exc_info=True)
        return None


def fetch_csv(csv_path: str, job_role: str) -> Union[List[Dict[str, Any]], None]:
    """
    Fetch jobs from a CSV file.
    """
    jobs_list = []
    try:
        with open(csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                if job_role.lower() in row['title'].lower():
                    job = Job(
                        timestamp=row.get('timestamp', 'Unknown'),
                        title=row.get('title', 'Unknown'),
                        company=row.get('company', 'Unknown'),
                        link=row.get('link', 'Unknown'),
                        job_type=row.get('job_type', 'Unknown'),
                        region=row.get('region', 'Unknown'),
                        salary=row.get('salary')
                    )
                    jobs_list.append(job)
        return [job.to_dict() for job in jobs_list]
    except Exception as e:
        logger.error("An error occurred while fetching data from CSV", exc_info=True)
        return None


# Function to clean and format salary
def clean_and_format_salary(salary):
    """
    Cleansing salary data.
    """
    if salary is None:
        return None

    # Patterns for different formats
    patterns = [
        r'\$([0-9,]+)\s*-\s*\$([0-9,]+)',  # Pattern for ranges like $215,000 - $320,000
        r'\b([0-9,]+)\b'  # Pattern for single numbers like 120000 USD
    ]

    # Clean number function
    def clean_number(num):
        return num.replace(',', '')

    for pattern in patterns:
        match = re.search(pattern, salary)
        if match:
            if len(match.groups()) == 2:
                start_value = clean_number(match.group(1))
                end_value = clean_number(match.group(2))
                return f"{start_value} - {end_value}"
            else:
                return clean_number(match.group(1))

    return None  # Return None if no pattern matches
