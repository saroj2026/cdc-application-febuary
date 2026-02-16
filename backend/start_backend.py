"""Start backend server with proper error handling."""
import sys
import os
import subprocess
import multiprocessing

# Fix for Windows multiprocessing spawn issue
# Set multiprocessing start method before any multiprocessing operations
if sys.platform == 'win32':
    try:
        multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
        # Already set, ignore
        pass

# Change to the backend directory
backend_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(backend_dir)

print("=" * 70)
print("Starting Backend Server")
print("=" * 70)
print(f"Working Directory: {os.getcwd()}")
print(f"Python: {sys.executable}")
print()

# Set PYTHONPATH to backend directory so imports work
env = os.environ.copy()
env['PYTHONPATH'] = backend_dir

try:
    print("Starting uvicorn server...")
    print("   Press Ctrl+C to stop")
    print("=" * 70)
    print()
    
    # Start uvicorn
    subprocess.run([
        sys.executable, '-m', 'uvicorn',
        'ingestion.api:app',
        '--host', '0.0.0.0',
        '--port', '8000',
        '--reload'
    ], env=env, check=True)
    
except KeyboardInterrupt:
    print("\n\nBackend stopped by user")
except subprocess.CalledProcessError as e:
    print(f"\nError starting backend: {e}")
    print("\nTrying to diagnose...")
    try:
        # Try importing the module to see if there's an import error
        import ingestion.api
        print("Module imports successfully")
    except Exception as import_error:
        print(f"Import error: {import_error}")
        import traceback
        traceback.print_exc()
except Exception as e:
    print(f"\nUnexpected error: {e}")
    import traceback
    traceback.print_exc()


