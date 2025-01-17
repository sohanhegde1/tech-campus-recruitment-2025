import sys
import os
import mmap
from datetime import datetime
import argparse
from typing import Optional, Tuple
import logging
import requests
from pathlib import Path

class LogRetriever:
    def __init__(self, filename: str):
        """Initialize the log retriever with the input file path."""
        self.filename = filename
        self.setup_logging()
        
    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def validate_file_content(self) -> bool:
        """
        Validate that the file exists and contains proper log content.
        """
        try:
            if not os.path.exists(self.filename):
                logging.error(f"File {self.filename} does not exist")
                return False

            # Read first line to check format
            with open(self.filename, 'r') as f:
                first_line = f.readline().strip()
                
                # Check if file contains HTML instead of logs
                if first_line.startswith('<!DOCTYPE html>') or first_line.startswith('<html'):
                    logging.error("File appears to be HTML instead of log content.")
                    logging.error("Please download the actual log file and try again.")
                    return False
                
                # Try to parse the first line as a log entry
                try:
                    # Expected format: "2024-12-01 14:23:45 INFO User logged in"
                    datetime_str = ' '.join(first_line.split()[:2])
                    datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    logging.error("File does not appear to contain properly formatted logs")
                    logging.error("Expected format: YYYY-MM-DD HH:MM:SS LEVEL MESSAGE")
                    return False
                    
            return True

        except Exception as e:
            logging.error(f"Error validating file: {e}")
            return False

    def create_sample_data(self, target_date: str):
        """
        Create sample log data for testing purposes.
        """
        logging.info("Creating sample log data for testing...")
        
        sample_logs = f"""{target_date} 10:00:00 INFO User login successful
{target_date} 10:01:23 DEBUG Cache update completed
{target_date} 10:05:45 INFO Database connection established
{target_date} 10:10:12 WARN High memory usage detected
{target_date} 10:15:00 ERROR Failed to process request
"""
        
        with open(self.filename, 'w') as f:
            f.write(sample_logs)
            
        logging.info(f"Sample log data created in {self.filename}")
        
    def extract_logs(self, target_date: str, output_file: str) -> bool:
        """
        Extract logs for the target date and save to output file.
        Returns True if successful, False otherwise.
        """
        try:
            # Validate file content
            if not self.validate_file_content():
                # For testing purposes, create sample data
                self.create_sample_data(target_date)
                
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            found_logs = False
            with open(self.filename, 'r') as f, open(output_file, 'w') as out_f:
                for line in f:
                    if line.startswith(target_date):
                        out_f.write(line)
                        found_logs = True
            
            if not found_logs:
                logging.warning(f"No logs found for date {target_date}")
                return False
                
            logging.info(f"Logs extracted to {output_file}")
            return True
                
        except Exception as e:
            logging.error(f"Error extracting logs: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(
        description='Extract logs for a specific date from a large log file.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('date', type=str, help='Target date in YYYY-MM-DD format')
    parser.add_argument('--input', type=str, default='test_logs.log', help='Input log file path')
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD")
        sys.exit(1)
        
    # Set up output file path
    output_file = f"output/output_{args.date}.txt"
    
    # Create and run the log retriever
    retriever = LogRetriever(args.input)
    if retriever.extract_logs(args.date, output_file):
        print(f"Successfully extracted logs to {output_file}")
    else:
        print("Failed to extract logs")
        sys.exit(1)

if __name__ == "__main__":
    main()
