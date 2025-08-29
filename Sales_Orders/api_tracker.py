import time
import threading

# Constants for rate limits
DAILY_LIMIT = 5000
MINUTE_LIMIT = 60
HOUR_LIMIT = 3600

LOCK = threading.Lock()  # Prevent race conditions in multithreading

# In-memory dictionary to track usage for each user
user_data = {}

def log_api_call(user_name):
    """Log an API call and enforce limits for a specific user."""
    with LOCK:
        now = time.time()

        # Initialize the user entry if it doesn't exist
        if user_name not in user_data:
            user_data[user_name] = {
                "api_calls": 0,
                "minute_calls": 0,
                "hour_calls": 0,
                "last_reset": now,
                "last_minute_reset": now,
                "last_hour_reset": now,
            }

        user = user_data[user_name]

        # Reset counters if limits exceeded
        if now - user["last_reset"] >= 86400:  # Reset daily limit after 24 hours
            user["api_calls"] = 0
            user["last_reset"] = now

        if now - user["last_minute_reset"] >= 60:  # Reset minute limit every 60 seconds
            user["minute_calls"] = 0
            user["last_minute_reset"] = now

        if now - user["last_hour_reset"] >= 3600:  # Reset hourly limit every hour
            user["hour_calls"] = 0
            user["last_hour_reset"] = now

        # Check if daily, minute or hour limits are reached
        if user["api_calls"] >= DAILY_LIMIT:
            print(f"Daily API limit reached for {user_name}. Sleeping for 1 hour...")
            time.sleep(3600)  # Sleep for 1 hour if daily limit reached
            user["api_calls"] = 0  # Reset after sleep to avoid blocking further requests
            return False

        if user["minute_calls"] >= MINUTE_LIMIT:
            print(f"Minute API limit reached for {user_name}. Sleeping for 1 minute...")
            time.sleep(60)  # Sleep for 1 minute if minute limit reached
            user["minute_calls"] = 0  # Reset after sleep
            return False

        # Increment counters for valid API calls
        user["api_calls"] += 1
        user["minute_calls"] += 1
        user["hour_calls"] += 1

        # Sleep for 1 second to ensure we don't exceed rate limits (3 calls per second)
        print(f"API Call Count for {user_name}: {user['api_calls']} (Minute: {user['minute_calls']}, Hour: {user['hour_calls']})")
        
        time.sleep(1)  # Sleep for 1 second between calls to avoid overloading the API

        return True  # Indicate success

def get_api_usage(user_name):
    """Get current API usage for a specific user."""
    with LOCK:
        if user_name in user_data:
            return user_data[user_name]
        else:
            return {"api_calls": 0, "minute_calls": 0, "hour_calls": 0}

def reset_tracker(user_name):
    """Reset the tracker manually for a specific user if needed."""
    with LOCK:
        user_data[user_name] = {
            "api_calls": 0,
            "minute_calls": 0,
            "hour_calls": 0,
            "last_reset": time.time(),
            "last_minute_reset": time.time(),
            "last_hour_reset": time.time(),
        }

def main():
    # Sample usage
    print("Starting the tracker script...")

    # Example of using specific usernames for tracking
    user_name = "AlbertRogerUK"  # Set the user for testing

    # Log an API call for the specific user
    result = log_api_call(user_name)
    if result:
        print("API call logged successfully.")
    else:
        print("API call limit reached.")

    # Get current usage for the specific user
    usage = get_api_usage(user_name)
    print(f"Current API usage for {user_name}: {usage}")

    # Reset tracker if needed for that specific user
    # reset_tracker(user_name)  # Uncomment this if you want to reset the tracker manually

if __name__ == "__main__":
    main()
    