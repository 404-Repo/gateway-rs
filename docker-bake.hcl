variable "VERSION" {
  default = "1.0.0"
}

variable "REPO_NAME" {
  default = "gateway-rs"
}

group "default" {
  targets = ["gen404-image"]
}

target "gen404-image" {
  platforms  = ["linux/amd64","linux/arm64"]
  context = "."
  dockerfile = "Dockerfile"
  tags = [
    "europe-docker.pkg.dev/gen-456515/${REPO_NAME}/${REPO_NAME}:${VERSION}",
  ]
}
