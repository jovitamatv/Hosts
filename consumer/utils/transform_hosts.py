import hashlib

class HostsTransformer:
    """A class used to transform, defang and hash hosts.
    Attributes:
        message (dict): The message containing source information and hosts to transform.
        logger (logging.Logger): Logger instance for logging.
    """

    def __init__(self, message, logger):
        """Initialize the HostsTransformer with message and a logger.
        Args:
            message (dict): The message containing source information and hosts to transform.
            logger (logging.Logger): Logger instance for logging.
        """
        self.message = message 
        self.logger = logger

    def defang_host(self, host):
        """Defang the host by replacing '.' with '[.]'.
        Args:
            host (str): The host to defang.
        Returns:
            str: The defanged host.
        """
        return host.replace('.', '[.]')

    def hash_host(self, host):
        """Hash the host using SHA-256.
        Args:
            host (str): The host to hash.
        Returns:
            str: The SHA-256 hash of the host in hexadecimal format.
        """
        return hashlib.sha256(host.encode()).hexdigest()
    

    def transform_hosts(self):
        """Transform the host information from the message.
        Returns:
            dict: A dictionary containing the transformed host information and retrieval time.
            None: If transformation fails.
        """
        try:
            transformed_record = {
                "defanged_host": self.defang_host(self.message["host"]),
                "hashed_host": self.hash_host(self.message["host"]),
                "category": self.message["category"],
                "source_name": self.message["name"],
                "source_url": self.message["url"],
                "retrieved_at": self.message["retrieval_time"]
            }
            return transformed_record
        except Exception as e:
            self.logger.error(f"Error transforming host from source {self.message['name']}: {e}; "
                              f"payload={self.message}", exc_info=True)
            return None

