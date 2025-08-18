import os
import subprocess
import threading
import time
import sys

docker_images = [
    # Módulo logística
    {
        "label": "Extração de logística",
        "name": "run-extracao-logistica",
        "path": "./src/logistica/extracao",
    },
    {
        "label": "Extração de logística 01",
        "name": "run-extracao-logistica-01",
        "path": "./src/logistica/logistica_01",
    },
    {
        "label": "Extração de logística 02",
        "name": "run-extracao-logistica-02",
        "path": "./src/logistica/logistica_02",
    },
    # Módulo metas individuais CBIOs
    {
        "label": "Extração metas CBIOs 2019",
        "name": "run-extracao-metas-cbios-2019-job",
        "path": "./src/metas_individuais_cbios/cbios_2019",
    },
    {
        "label": "Extração metas CBIOs 2020",
        "name": "run-extracao-metas-cbios-2020-job",
        "path": "./src/metas_individuais_cbios/cbios_2020",
    },
    {
        "label": "Extração metas CBIOs 2021",
        "name": "run-extracao-metas-cbios-2021-job",
        "path": "./src/metas_individuais_cbios/cbios_2021",
    },
    {
        "label": "Extração metas CBIOs 2022",
        "name": "run-extracao-metas-cbios-2022-job",
        "path": "./src/metas_individuais_cbios/cbios_2022",
    },
    {
        "label": "Extração metas CBIOs 2023",
        "name": "run-extracao-metas-cbios-2023-job",
        "path": "./src/metas_individuais_cbios/cbios_2023",
    },
    {
        "label": "Extração metas CBIOs 2024",
        "name": "run-extracao-metas-cbios-2024-job",
        "path": "./src/metas_individuais_cbios/cbios_2024",
    }
]

PROJECT_ID = "ext-ecole-biomassa-468317"
ARTIFACT_REPO = "ar-juridico-process-anp-datalake"

artifact_registry_base_url = f"us-central1-docker.pkg.dev/{PROJECT_ID}/{ARTIFACT_REPO}/"

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
        "-t", f"{artifact_registry_base_url}{image_name}",
        image_path
    ]

    # Push command
    push_command = [
        "docker", "push",
        f"{artifact_registry_base_url}{image_name}"
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


print("\033[2J\033[H", end="")
print("🏗️ Starting Docker images processing...")

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
print(f"\033[{len(docker_images) + 2};1H🏁 All Docker images processing completed!")
