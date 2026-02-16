"""Start both backend and frontend servers."""

import subprocess
import sys
import time
import os
import requests

print("=" * 70)
print("Starting Backend and Frontend Servers")
print("=" * 70)

# Step 1: Check for existing backend on port 8000
print("\n1. Checking for existing backend on port 8000...")

try:
    response = requests.get("http://localhost:8000/health", timeout=2)
    if response.status_code == 200:
        print("   ⚠️  Backend is already running on port 8000")
        print("   💡 If you want to restart it, please stop it manually first")
        print("   ℹ️  Continuing to start frontend...")
    else:
        print("   ℹ️  No active backend found on port 8000")
except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
    print("   ℹ️  No backend found on port 8000 - good to start")
except Exception as e:
    print(f"   ℹ️  Could not check port 8000: {e}")

# Wait for processes to terminate
print("\n2. Waiting 2 seconds...")
time.sleep(2)

# Step 2: Start Backend
print("\n3. Starting Backend Server...")
script_dir = os.path.dirname(os.path.abspath(__file__))
backend_dir = os.path.join(script_dir, "backend")

backend_started = False
if sys.platform == "win32":
    try:
        # Start backend in new window
        cmd = f'cd /d "{backend_dir}" && set PYTHONPATH={backend_dir} && python -m uvicorn ingestion.api:app --reload --host 0.0.0.0 --port 8000'
        subprocess.Popen(
            ["cmd", "/c", "start", "cmd", "/k", "title Backend Server && " + cmd],
            shell=True
        )
        backend_started = True
        print("   ✅ Backend started in new window")
    except Exception as e:
        print(f"   ❌ Failed to start backend: {e}")
else:
    try:
        env = os.environ.copy()
        env['PYTHONPATH'] = backend_dir
        with open("backend.log", "w") as log_file:
            process = subprocess.Popen(
                ["python", "-m", "uvicorn", "ingestion.api:app", "--reload", "--host", "0.0.0.0", "--port", "8000"],
                cwd=backend_dir,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT
            )
        backend_started = True
        print(f"   ✅ Backend started (PID: {process.pid})")
    except Exception as e:
        print(f"   ❌ Failed to start backend: {e}")

# Step 3: Wait and verify backend
if backend_started:
    print("\n4. Waiting 8 seconds for backend to initialize...")
    time.sleep(8)
    
    print("5. Verifying backend...")
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print("   ✅ Backend is healthy and responding!")
        else:
            print(f"   ⚠️  Backend responded with status {response.status_code}")
    except requests.exceptions.Timeout:
        print("   ⚠️  Backend is starting but not yet ready (timeout)")
        print("   This is normal - it may take a few more seconds")
    except requests.exceptions.ConnectionError:
        print("   ⚠️  Backend is starting but not yet ready (connection refused)")
        print("   This is normal - it may take a few more seconds")
    except Exception as e:
        print(f"   ⚠️  Could not verify backend: {e}")

# Step 4: Check if Node.js is available
print("\n6. Checking Node.js...")
node_available = False
try:
    result = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=3)
    if result.returncode == 0:
        node_version = result.stdout.strip()
        print(f"   ✅ Node.js found: {node_version}")
        node_available = True
    else:
        print("   ❌ Node.js not found")
except FileNotFoundError:
    print("   ❌ Node.js not installed")
except Exception as e:
    print(f"   ⚠️  Error checking Node.js: {e}")

# Step 5: Start Frontend
if node_available:
    print("\n7. Starting Frontend Server...")
    
    frontend_dir = os.path.join(script_dir, "frontend")
    if not os.path.exists(frontend_dir):
        print(f"   ❌ Frontend directory not found: {frontend_dir}")
    else:
        # Check if node_modules exists
        node_modules_path = os.path.join(frontend_dir, "node_modules")
        if not os.path.exists(node_modules_path):
            print("   📦 Installing frontend dependencies (this may take a few minutes)...")
            try:
                install_result = subprocess.run(
                    ["npm", "install"],
                    cwd=frontend_dir,
                    timeout=300,  # 5 minutes timeout
                    capture_output=True,
                    text=True
                )
                if install_result.returncode == 0:
                    print("   ✅ Dependencies installed")
                else:
                    print(f"   ⚠️  npm install had issues: {install_result.stderr[:200]}")
            except subprocess.TimeoutExpired:
                print("   ⚠️  npm install timed out (still running in background)")
            except Exception as e:
                print(f"   ⚠️  Error installing dependencies: {e}")
        
        # Start frontend
        try:
            if sys.platform == "win32":
                cmd = f'cd /d "{frontend_dir}" && npm run dev'
                subprocess.Popen(
                    ["cmd", "/c", "start", "cmd", "/k", "title Frontend Server && " + cmd],
                    shell=True
                )
                print("   ✅ Frontend started in new window")
            else:
                with open("frontend.log", "w") as log_file:
                    process = subprocess.Popen(
                        ["npm", "run", "dev"],
                        cwd=frontend_dir,
                        stdout=log_file,
                        stderr=subprocess.STDOUT
                    )
                print(f"   ✅ Frontend started (PID: {process.pid})")
        except Exception as e:
            print(f"   ❌ Failed to start frontend: {e}")
else:
    print("\n7. Skipping frontend (Node.js not available)")
    print("   Please install Node.js from https://nodejs.org/")

# Summary
print("\n" + "=" * 70)
print("✅ Startup Process Completed!")
print("=" * 70)
print("\n📝 Services:")
print("   Backend API: http://localhost:8000")
print("   API Docs: http://localhost:8000/docs")
if node_available:
    print("   Frontend UI: http://localhost:3000")
print("\n💡 Both services are running in separate windows.")
print("   Check those windows for any errors or logs.")
print("\n⚠️  If services didn't start, check the windows for error messages.")


