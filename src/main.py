

import sys
import os
import mmap
from datetime import datetime
import argparse
from typing import Optional, Tuple
import logging

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
        
    def binary_search_position(self, mm: mmap.mmap, target_date: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Perform binary search to find the approximate start position of the target date.
        Returns a tuple of (start_position, end_position).
        """
        file_size = len(mm)
        left, right = 0, file_size - 1
        
        # Estimated size of one day's worth of logs
        # Since logs are evenly distributed, we can estimate positions
        day_size = file_size // 365  # Approximate size for one day
        
        # Quick estimation of initial position based on target date
        try:
            current_year = datetime.now().year
            target_datetime = datetime.strptime(target_date, '%Y-%m-%d')
            days_diff = (datetime(target_datetime.year, target_datetime.month, target_datetime.day) - 
                        datetime(current_year, 1, 1)).days
            initial_pos = (days_diff * day_size)
            
            # Adjust to nearest newline
            initial_pos = self.find_nearest_line_start(mm, initial_pos)
            
            # Search around this position
            return self.refine_search(mm, initial_pos, target_date, day_size)
            
        except ValueError as e:
            logging.error(f"Error parsing date: {e}")
            return None, None
            
    def find_nearest_line_start(self, mm: mmap.mmap, pos: int) -> int:
        """Find the start of the nearest line."""
        file_size = len(mm)
        
        # Bound checking
        if pos >= file_size:
            pos = file_size - 1
        if pos < 0:
            pos = 0
            
        # Search backwards for newline
        while pos > 0 and mm[pos - 1:pos] != b'\n':
            pos -= 1
            
        return pos
        
    def refine_search(self, mm: mmap.mmap, initial_pos: int, target_date: str, day_size: int) -> Tuple[int, int]:
        """Refine the search around the initial position."""
        file_size = len(mm)
        
        # Search forward for start position
        start_pos = initial_pos
        while start_pos > 0:
            mm.seek(start_pos)
            line = mm.readline().decode('utf-8').strip()
            if not line.startswith(target_date):
                break
            start_pos = self.find_nearest_line_start(mm, start_pos - day_size // 10)
            
        # Search forward for end position
        end_pos = initial_pos
        while end_pos < file_size:
            mm.seek(end_pos)
            line = mm.readline().decode('utf-8').strip()
            if not line.startswith(target_date):
                break
            end_pos = self.find_nearest_line_start(mm, end_pos + day_size // 10)
            
        return start_pos, end_pos
        
    def extract_logs(self, target_date: str, output_file: str) -> bool:
        """
        Extract logs for the target date and save to output file.
        Returns True if successful, False otherwise.
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(self.filename, 'rb') as f:
                # Memory-map the file for efficient reading
                mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
                
                # Find approximate positions for the target date
                start_pos, end_pos = self.binary_search_position(mm, target_date)
                
                if start_pos is None or end_pos is None:
                    logging.error("Failed to find log positions")
                    return False
                
                # Extract and write matching logs
                with open(output_file, 'w') as out_f:
                    mm.seek(start_pos)
                    while mm.tell() < end_pos:
                        line = mm.readline().decode('utf-8')
                        if line.startswith(target_date):
                            out_f.write(line)
                            
                mm.close()
                return True
                
        except Exception as e:
            logging.error(f"Error extracting logs: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Extract logs for a specific date from a large log file.')
    parser.add_argument('date', help='Target date in YYYY-MM-DD format')
    parser.add_argument('--input', default='test_logs.log', help='Input log file path')
    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.date, '%Y-%m-%d')
    except ValueError:
        logging.error("Invalid date format. Please use YYYY-MM-DD")
        sys.exit(1)

    output_file = f"output/output_{args.date}.txt"
    
    retriever = LogRetriever(args.input)
    if retriever.extract_logs(args.date, output_file):
        logging.info(f"Successfully extracted logs to {output_file}")
    else:
        logging.error("Failed to extract logs")
        sys.exit(1)

if __name__ == "__main__":
    main()
