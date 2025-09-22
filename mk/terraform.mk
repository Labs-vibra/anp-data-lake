run-terraform-prod:
	cd terraform && terraform apply -var="is_prod=true" && cd ..

run-terraform-dev:
	cd terraform && terraform apply && cd ..

validate-terraform:
	cd terraform && \
	terraform validate && \
	terraform fmt && \
	cd ..

create-artifact-registry-prod:
	cd terraform && terraform apply -target=google_artifact_registry_repository.anp_repo_etl -var="is_prod=true" && cd ..

create-artifact-registry:
	cd terraform && terraform apply -target=google_artifact_registry_repository.anp_repo_etl && cd ..