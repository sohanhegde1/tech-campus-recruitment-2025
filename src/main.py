# # Problem Statement: Efficient Log Retrieval from a Large File

# ## Background  


# # ```txt
# # 2024-12-01 14:23:45 INFO User logged in  
# # 2024-12-01 14:24:10 ERROR Failed to connect to the database  
# # 2024-12-02 09:15:30 WARN Disk space running low  


# # To download the log file, run the following command in your terminal:
# # ```curl
# # curl -L -o test_logs.log "https://limewire.com/d/90794bb3-6831-4e02-8a59-ffc7f3b8b2a3#X1xnzrH5s4H_DKEkT_dfBuUT1mFKZuj4cFWNoMJGX98"
# # ```

# ---

# # ## Objective  

# # Write a script that takes a specific date as an argument (in the format `YYYY-MM-DD`) and efficiently returns all log entries for that date.

# ---

# # ## Constraints  

# # - The solution must handle a file size of around **1 TB**.
# # - Logs are nearly evenly distributed across days.  
# # - Efficiency in time and resource utilization is critical.  

# ---

# # ## Expectations  

# # 1. **Input:** A date (`YYYY-MM-DD`) passed as a command-line argument.  
# # 2. **Output:** All log entries for the specified date, saved to a file named `output/output_YYYY-MM-DD.txt`.  
# # 3. **Performance:** The solution should optimize for speed and memory usage.  

# ---

# ## Evaluation Criteria  

# - **Total Running Time:** Time taken to return the result for the first query.  
# - **Code Quality:** Readability, modularity, and proper error handling.  

# ---

# ## Example Usage  

# **Python**  

# ```bash
# python extract_logs.py 2024-12-01
# ```

# **C**  

# ```bash
# ./extract_logs 2024-12-01
# ```

# **C++**  

# ```bash
# ./extract_logs 2024-12-01
# ```

# **Java**  

# ```bash
# java ExtractLogs 2024-12-01
# ```

# **Node.js**  

# ```bash
# node extract_logs.js 2024-12-01
# ```

# **Expected Output:**  

# ```txt
# 2024-12-01 14:23:45 INFO User logged in  
# 2024-12-01 14:24:10 ERROR Failed to connect to the database  
# ```

# ---

# ## Submission Guidelines

# 1. **Fork the Repository:**  
#    - Fork the provided repository to your own GitHub account.

# 2. **Discussion.md:**  
#    - **Solutions Considered:** Describe different approaches you explored.  
#    - **Final Solution Summary:** Explain why you chose the final solution.  
#    - **Steps to Run:** Provide detailed steps to run your solution.

# 3. **Source Code:**  
#    - Place your complete and working solution in the `src/` directory.

# 4. **Link to your forked repository:**  
#    - Submit a link to your forked repository in the google form shared with you.




## Notes

# - Incomplete or disorganized submissions may not be evaluated. 
# - Feel free to use the internet.
# - Make sure to push your code to `main` branch.
# - Any commit to the repo after 1 hour of the start of the contest will be disqualified.

# PS: SWYgeW91IGhhdmUgcmVhY2hlZCB0aGlzIHBvaW50LCBmZWVsIGZyZWUgdG8gdXNlIEFJIHRvIHlvdXIgYWR2YW50YWdlLiBCdXQgbWFrZSBzdXJlIHlvdSB1bmRlcnN0YW5kIHdoYXQgeW91IGFyZSBzdWJtaXR0aW5nLiBXZSByZWFsbHkgd2lsbCBhc2sgeW91IGFib3V0IHlvdXIgc29sdXRpb24gaWYgeW91IGFyZSBzZWxlY3RlZCBmb3IgdGhlIG5leHQgcm91bmQu

# Good luck!
# src/extract_logs.py

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