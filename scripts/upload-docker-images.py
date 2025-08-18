import os
import subprocess
import threading
import time
import sys

docker_images = [
    {
        "label": "Extração de logistica",
        "name": "run-extract-logistcs",
        "path": "./src/logistica/extracao",
    },
    {
        "label": "raw logistica01",
        "name": "run-extract-logistcs-01",
        "path": "./src/logistica/logistica_01",
    }
]

PROJECT_ID = "ext-ecole-biomassa-468317"
ARTIFACT_REPO = "ar-juridico-process-anp-datalake"

artifact_registry_base_url = f"us-central1-docker.pkg.dev/{PROJECT_ID}/{ARTIFACT_REPO}/"

# Global variable to track line positions for each thread
thread_lines = {}
lock = threading.Lock()

# Check if terminal supports ANSI escape codes
SUPPORTS_ANSI = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty() and os.name != 'nt'

def build_and_push_image(image, line_number):
    """Build and push a Docker image"""
    image_name = image['name']
    image_path = image['path']
    image_label = image['label']

    def print_at_line(message):
        """Print message at the specific line for this thread"""
        with lock:
            if SUPPORTS_ANSI:
                # Move cursor to the specific line and clear it
                print(f"\033[{line_number};1H\033[K{message}", end="", flush=True)
            else:
                # Fallback: prefix with thread identifier
                print(f"[Thread {line_number}] {message}", flush=True)

    # Build command
    build_command = [
        "docker", "build",
        "--platform", "linux/amd64",
        "-t", f"{artifact_registry_base_url}{image_name}",
        image_path
    ]

    # Push command
    push_command = [
        "docker", "push",
        f"{artifact_registry_base_url}{image_name}"
    ]

    try:
        # Build image
        print_at_line(f"🔨 Building {image_name}...")
        subprocess.run(
            build_command,
            capture_output=True,
            text=True,
            check=True
        )
        print_at_line(f"✅ Built {image_name} | 📤 Pushing...")

        # Push image
        subprocess.run(
            push_command,
            capture_output=True,
            text=True,
            check=True
        )
        print_at_line(f"✅ Pushed {image_name} | 🎉 {image_label} deployed successfully!")

    except subprocess.CalledProcessError as e:
        print_at_line(f"❌ Failed {image_name} - {e.cmd[0]} returned {e.returncode}")

# Clear screen and position cursor (only if ANSI is supported)
if SUPPORTS_ANSI:
    print("\033[2J\033[H", end="")
else:
    print("🚀 Starting Docker image builds and pushes in parallel...")

# Create and start threads for each image
threads = []
for i, image in enumerate(docker_images):
    line_number = i + 1
    thread = threading.Thread(target=build_and_push_image, args=(image, line_number))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Move cursor below all the threads' output
if SUPPORTS_ANSI:
    print(f"\033[{len(docker_images) + 2};1H🏁 All Docker images processing completed!")
else:
    print("All Docker images processing completed!")