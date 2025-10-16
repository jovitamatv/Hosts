import re
import requests
from datetime import datetime

class HostsExtractor:
    """
    A class used to extract hosts from data sources.
    
    Attributes:
        source_info (dict): Information about the data source.
        logger (logging.Logger): Logger instance for logging.
    """

    RESERVED_IPS = [
            "0.0.0.0",
            "127.0.0.1",
            "255.255.255.255",
            "::1",
            "fe80::1%lo0",
            "ff00::0",
            "ff02::1",
            "ff02::2",
            "ff02::3",
            "localhost",
            "localhost.localdomain",
            "local",
            "localdomain",
            "broadcasthost",
            "ip6-localhost",
            "ip6-loopback",
            "ip6-localnet",
            "ip6-mcastprefix",
            "ip6-allnodes",
            "ip6-allrouters",
            "ip6-allhosts"
        ]

    IPV4_REGEX = re.compile(
        r'(?<![a-zA-Z0-9.\-])(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}'
        r'(?:25[0-5]|2[0-4]\d|[01]?\d\d?)(?![a-zA-Z0-9.\-])'
    )
    
    IPV6_REGEX = re.compile(
        r'(?<![a-zA-Z0-9:\-])(?:'
        r'(?:[A-Fa-f0-9]{1,4}:){7}[A-Fa-f0-9]{1,4}|'
        r'(?:[A-Fa-f0-9]{1,4}:){1,7}:|'
        r'(?:[A-Fa-f0-9]{1,4}:){1,6}:[A-Fa-f0-9]{1,4}|'
        r'(?:[A-Fa-f0-9]{1,4}:){1,5}(?::[A-Fa-f0-9]{1,4}){1,2}|'
        r'(?:[A-Fa-f0-9]{1,4}:){1,4}(?::[A-Fa-f0-9]{1,4}){1,3}|'
        r'(?:[A-Fa-f0-9]{1,4}:){1,3}(?::[A-Fa-f0-9]{1,4}){1,4}|'
        r'(?:[A-Fa-f0-9]{1,4}:){1,2}(?::[A-Fa-f0-9]{1,4}){1,5}|'
        r'[A-Fa-f0-9]{1,4}:(?:(?::[A-Fa-f0-9]{1,4}){1,6})|'
        r':(?:(?::[A-Fa-f0-9]{1,4}){1,7}|:)'
        r')(?:%[a-zA-Z0-9]+)?(?![a-zA-Z0-9:\-])',
        re.IGNORECASE
    )
    
    HOSTNAME_REGEX = re.compile(
        r'\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+' 
        r'[a-zA-Z](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\b'
    )

    def __init__(self, source_info, logger):
        """
        Initialize the HostsExtractor with source information and a logger.
        
        Args:
            source_info (dict): Information about the data source.
            logger (logging.Logger): Logger instance for logging.
        """
        self.source_info = source_info
        self.logger = logger    
    
    def clean_text(self, text):
        """
        Clean the text by removing comments and reserved IP addresses.
        
        Args:
            text (str): The text to clean.
        
        Returns:
            str: The cleaned text.
        """

        COMMENT_REGEX = re.compile(r'(?:#|!|//)[^\n]*')
        
        IP_REGEX = re.compile(
            r'(?<![a-zA-Z0-9.\-])(?:' + 
            '|'.join([re.escape(ip) for ip in self.RESERVED_IPS]) + 
            r')(?![a-zA-Z0-9.\-])'
        )

        return IP_REGEX.sub(' ', COMMENT_REGEX.sub('', text))
    
    def retrieve_web_content(self, url):
        """
        Retrieve the content of a web resource.
        
        Args:
            url (str): The URL of the web resource.
        
        Returns:
            str: The content of the web resource.
        """
        
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            text = r.text
            self.logger.info(f"Successfully retrieved content from {self.source_info['name']}")
            return text
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error while retrieving {self.source_info['name']}: {e}", exc_info=True)
            return ""
        except requests.RequestException as e:
            self.logger.error(f"Failed to retrieve content from {self.source_info['name']}: {e}", exc_info=True)
            return ""

    def retrieve_file_content(self, path):
        """
        Retrieve the content of a file based on its path.
        
        Args:
            path (str): The file path.
        
        Returns:
            str: The content of the file.
        """

        try:
            with open(path, 'r') as file:
                text = file.read()
            self.logger.info(f"Successfully read content from {self.source_info['name']}")
            return text
        except IOError as e:
            self.logger.error(f"Failed to read content from {self.source_info['name']}: {e}", exc_info=True)
            return ""
        
    def retrieve_content(self):
        """
        Retrieve the content based on source type.

        Returns:
            str: The content of the file.
            str: The timestamp when the content was retrieved.
        """
        source_type = self.source_info.get('type')
        source_url = self.source_info.get('url')

        self.logger.info(f"Retrieving content from {self.source_info['name']}")
        if source_type == 'http':
            content = self.retrieve_web_content(source_url)
        elif source_type == 'file':
            content = self.retrieve_file_content(source_url)
        else:
            self.logger.error(f"Unsupported source type '{source_type}' for source '{self.source_info['name']}'")
            return "", None
        return content, str(datetime.now())

    def extract_hosts(self):
        """
        Extract hosts from the source content.
        
        Returns: 
            list: A list of unique hosts extracted from the content.
            str: The timestamp when the content was retrieved.
        """
        source_content, retrieval_time = self.retrieve_content()
        if not source_content:
            return [], retrieval_time
        
        cleaned_text = self.clean_text(source_content) 

        ipv4s = self.IPV4_REGEX.findall(cleaned_text)
        ipv6s = self.IPV6_REGEX.findall(cleaned_text)
        hostnames = self.HOSTNAME_REGEX.findall(cleaned_text)

        return list(set(ipv4s + ipv6s + hostnames)), retrieval_time
