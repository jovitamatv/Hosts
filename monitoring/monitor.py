import time
import os
import sys
from datetime import datetime
from utils.email_notifier import send_email
class StatusLogMonitor:
    """Monitor and display the status of Kafka pipeline services."""
    
    def __init__(self, filepath, services):
        print(f"Initializing monitor with {len(services)} services", flush=True)
        self.filepath = filepath
        self.services = services
        self.last_status = {}
        self.first_run = True

    def read_last_line(self, log_path):
        """Read last line from file."""
        try:
            with open(log_path, 'r') as f:
                lines = f.readlines()
                return lines[-1].strip() if lines else "No data yet"
        except FileNotFoundError:
            return "Not started"
        except Exception as e:
            return f"Error: {e}"

    def check_failed(self, log_path):
        """
        Check if last line indicates failure.

        Args:
            log_path (str): Path to the log file.

        Returns:
            bool: True if last line indicates failure, False otherwise.
        
        """
        try:
            with open(log_path, 'r') as f:
                return True if 'STATUS: failed' in f.readlines()[-1] else False
        except:
            return False

    def has_status_changed(self, service_name, current_status):
        """Check if status has changed since last check."""
        if self.first_run:
            self.last_status[service_name] = current_status
            return True
        
        last_status = self.last_status.get(service_name)
        
        if last_status != current_status:
            self.last_status[service_name] = current_status
            return True
        
        return False
    
    def alert_email(self, service_name, current_status):
        """
        Send email alert. Used to send email if errors are found.
        Args:
            service_name (str): Name of the service.
            current_status (str): Current status line from log.
        Returns:
            bool: True if email sent successfully, False otherwise.
        """
        
        subject = f"ALERT: {service_name} encountered errors"
        body = f"The service '{service_name}' has reported errors in its latest status:\n\n{current_status}\n" 
        body += "\nPlease investigate."
        email_sent = send_email(subject, body)
        return email_sent


    def monitor_pipeline(self):
        """Monitor all services continuously."""
        print(f"Starting monitoring loop...", flush=True)
        print(f"Services: {list(self.services.keys())}", flush=True)
        print("=" * 100, flush=True)
        
        check_count = 0
        try:
            while True:
                check_count += 1
                print(f"\n[Check #{check_count}] {datetime.now().strftime('%H:%M:%S')}", flush=True)
                
                any_changes = False 
                
                for service_name, log_file in self.services.items():
                    log_path = f"{self.filepath}/{log_file}"
                    
                    try:
                        current_status = self.read_last_line(log_path)
                        if self.has_status_changed(service_name, current_status):
                            any_changes = True
                            print(f"  🔹 {service_name}: {current_status}", flush=True)
                            
                            if self.check_failed(log_path):
                                print(f"     ⚠️  Errors found", flush=True)
                            else:
                                print(f"     ✅ No errors", flush=True)

                            if "STATUS: completed_with_errors" in current_status:
                                email_sent = self.alert_email(service_name, current_status)
                                if email_sent:
                                    print(f"     📧 Alert email sent to team", flush=True)
                                else:
                                    print(f"     ❌ Failed to send alert email", flush=True)
                    
                    except Exception as e:
                        print(f"  ❌ ERROR checking {service_name}: {e}", flush=True)
                
                if self.first_run:
                    print("✅ Initial check complete", flush=True)
                    self.first_run = False
                if not any_changes:
                    print("  No changes detected", flush=True)
                print(f"Sleeping 10 seconds...", flush=True)
                time.sleep(10)
            
        except KeyboardInterrupt:
            print("\nMonitor stopped by user", flush=True)
        except Exception as e:
            print(f"\nFATAL ERROR: {e}", flush=True)
            import traceback
            traceback.print_exc()
            sys.exit(1)

def parse_monitor_services(services_str):
    """Parse MONITOR_SERVICES environment variable."""
    services = {}
    if not services_str:
        return services
    
    for pair in services_str.split(','):
        pair = pair.strip()
        if ':' in pair:
            service, logfile = pair.split(':', 1)
            services[service.strip()] = logfile.strip()
    
    print(f"Parsed {len(services)} services", flush=True)
    return services

if __name__ == "__main__":
    try:
        print("=" * 100, flush=True)
        print("MONITOR SCRIPT STARTING", flush=True)
        print(f"Time: {datetime.now()}", flush=True)
        print("=" * 100, flush=True)

        print("Reading environment variables...", flush=True)
        log_dir = os.getenv("PROCESS_LOG_DIRS")
        services = os.getenv("MONITOR_SERVICES")
        
        print(f"PROCESS_LOG_DIRS = {log_dir}", flush=True)
        print(f"MONITOR_SERVICES = {services}", flush=True)
        
        if not log_dir:
            print("ERROR: PROCESS_LOG_DIRS not set!", flush=True)
            sys.exit(1)
        
        if not services:
            print("ERROR: MONITOR_SERVICES not set!", flush=True)
            sys.exit(1)
        
        parsed_services = parse_monitor_services(services)
        
        if not parsed_services:
            print("ERROR: No services parsed!", flush=True)
            sys.exit(1)

        monitor = StatusLogMonitor(log_dir, parsed_services)
        print(f"Starting monitor...", flush=True)
        monitor.monitor_pipeline()
        
    except Exception as e:
        print(f"FATAL ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)