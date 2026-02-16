
import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:8000"
LOGIN_URL = f"{BASE_URL}/api/v1/auth/login"
RESTART_URL = f"{BASE_URL}/api/v1/monitoring/event-logger/restart"

def restart_event_logger():
    # 1. Login to get token
    logger.info("Logging in as admin...")
    try:
        login_data = {
            "email": "admin@example.com",
            "password": "admin123"
        }
        
        # API expects JSON body with email/password
        response = requests.post(LOGIN_URL, json=login_data)
        
        if response.status_code != 200:
            logger.error(f"Login failed: {response.status_code} - {response.text}")
            return
            
        token_data = response.json()
        access_token = token_data.get("access_token")
        
        if not access_token:
            logger.error("No access token returned")
            return
            
        logger.info("Login successful")
        
        # 2. Call restart endpoint
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        
        logger.info("Triggering monitoring event logger restart...")
        response = requests.post(RESTART_URL, headers=headers)
        
        if response.status_code == 200:
            logger.info("✅ Restart successful!")
            logger.info(json.dumps(response.json(), indent=2))
        else:
            logger.error(f"❌ Restart failed: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    restart_event_logger()
