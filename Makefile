# Makefile
-include .env
-include mk/environment.mk
-include mk/docker.mk
-include mk/uploads.mk
-include mk/terraform.mkfile

.DEFAULT_GOAL := all