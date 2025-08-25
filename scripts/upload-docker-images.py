import os
import subprocess
import threading
import time
import sys
from docker_images_config import DOCKER_IMAGES, ARTIFACT_REGISTRY_BASE_URL
from dotenv import load_dotenv

load_dotenv()

print(os.getenv("GOOGLE_CLOUD_PROJECT"))
exit(0)

thread_lines = {}
lock = threading.Lock()

def build_and_push_image(image, line_number):
    """Build and push a Docker image"""
    image_name = image['name']
    image_path = image['path']
    image_label = image['label']

    # Spinner characters
    spinner_chars = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    spinner_index = 0

    def print_at_line(message, show_spinner=False):
        """Print message at the specific line for this thread"""
        nonlocal spinner_index
        with lock:
            if show_spinner:
                spinner = spinner_chars[spinner_index % len(spinner_chars)]
                spinner_index += 1
                full_message = f"{spinner} {message}"
            else:
                full_message = message

            print(f"\033[{line_number};1H\033[K{full_message}", end="", flush=True)

    # Build command
    build_command = [
        "docker", "build",
        "--platform", "linux/amd64",
        "-t", f"{ARTIFACT_REGISTRY_BASE_URL}{image_name}",
        image_path
    ]

    # Push command
    push_command = [
        "docker", "push",
        f"{ARTIFACT_REGISTRY_BASE_URL}{image_name}"
    ]

    try:
        # Build image with spinner
        print_at_line(f"Building {image_label}...", show_spinner=True)

        # Start build process
        build_process = subprocess.Popen(
            build_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Show spinner while building
        while build_process.poll() is None:
            print_at_line(f"Building {image_label}...", show_spinner=True)
            time.sleep(0.1)

        # Check if build was successful
        if build_process.returncode != 0:
            raise subprocess.CalledProcessError(build_process.returncode, build_command)

        print_at_line(f"✅ Built {image_label} | 📤 Pushing...", show_spinner=False)

        # Push image with spinner
        push_process = subprocess.Popen(
            push_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Show spinner while pushing
        while push_process.poll() is None:
            print_at_line(f"✅ Built {image_label} | Pushing...", show_spinner=True)
            time.sleep(0.1)

        # Check if push was successful
        if push_process.returncode != 0:
            raise subprocess.CalledProcessError(push_process.returncode, push_command)

        print_at_line(f"✅ Pushed {image_name} | 🎉 {image_label} deployed successfully!", show_spinner=False)

    except subprocess.CalledProcessError as e:
        print_at_line(f"❌ Failed {image_name} - {e.cmd[0] if e.cmd else 'unknown'} returned {e.returncode}", show_spinner=False)



# Timer start
start_time = time.time()

print("\033[2J\033[H", end="")
print("🏗️ Starting Docker images processing...")

# Create and start threads for each image
threads = []
for i, image in enumerate(DOCKER_IMAGES):
    line_number = i + 1
    thread = threading.Thread(target=build_and_push_image, args=(image, line_number))
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()


# Move cursor below all the threads' output
print(f"\033[{len(DOCKER_IMAGES) + 2};1H🏁 All Docker images processing completed!")

# Timer end and print elapsed time
elapsed = time.time() - start_time
print(f"Total execution time: {elapsed:.2f} seconds")
