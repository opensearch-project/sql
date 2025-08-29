#!/bin/bash

# Utility functions for publishing artifacts to Maven and managing commit mappings

set -e

# Flag to disable commit mapping functionality
# Set to "true" to disable commit mapping operations
DISABLE_COMMIT_MAPPING="${DISABLE_COMMIT_MAPPING:-true}"

# Function to execute curl commands with retry and error handling
execute_curl_with_retry() {
  local url="$1"
  local method="$2"
  local output_file="$3"
  local upload_file="$4"
  local max_retries=3
  local retry_count=0
  local sleep_time=10

  while [ $retry_count -lt $max_retries ]; do
    echo "Attempting curl request to ${url} (attempt $((retry_count + 1))/${max_retries})"

    local curl_cmd="curl -s -u \"${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}\""
    local http_code

    case "$method" in
      "GET")
        if [ -n "$output_file" ]; then
          curl_cmd="$curl_cmd -o \"$output_file\""
        fi
        curl_cmd="$curl_cmd \"$url\""
        ;;
      "PUT")
        curl_cmd="$curl_cmd --upload-file \"$upload_file\" \"$url\""
        ;;
      "HEAD")
        curl_cmd="$curl_cmd -I \"$url\""
        ;;
    esac

    echo "Executing: $curl_cmd"
    if eval $curl_cmd; then
      http_code=$(curl -s -o /dev/null -w "%{http_code}" -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" "$url")
      if [[ "$http_code" =~ ^[23] ]]; then
        echo "Request successful (HTTP $http_code)"
        return 0
      else
        echo "Request failed with HTTP code: $http_code"
      fi
    else
      echo "Curl command failed"
    fi

    retry_count=$((retry_count + 1))
    if [ $retry_count -lt $max_retries ]; then
      echo "Retrying in ${sleep_time} seconds..."
      sleep $sleep_time
      sleep_time=$((sleep_time * 2))  # Exponential backoff
    fi
  done

  echo "All retry attempts failed for ${url}"
  return 1
}

# Function to generate checksums for Maven artifacts
generate_checksums() {
  local base_path="${1:-${HOME}/.m2/repository/org/opensearch/}"

  echo "Generating checksums for artifacts in ${base_path}"

  # Generate SHA checksums for POM files
  for i in `find "${base_path}" -name "*.pom" -type f`; do
    sha512sum "$i" | awk '{print $1}' >> "$i.sha512"
    sha256sum "$i" | awk '{print $1}' >> "$i.sha256"
  done

  # Generate SHA checksums for JAR files
  for i in `find "${base_path}" -name "*.jar" -type f`; do
    sha512sum "$i" | awk '{print $1}' >> "$i.sha512"
    sha256sum "$i" | awk '{print $1}' >> "$i.sha256"
  done

  # Generate SHA checksums for ZIP files
  for i in `find "${base_path}" -name "*.zip" -type f`; do
    sha512sum "$i" | awk '{print $1}' >> "$i.sha512"
    sha256sum "$i" | awk '{print $1}' >> "$i.sha256"
  done

  echo "Checksum generation completed"
}

# Function to publish artifacts to Maven repository
publish_to_maven() {
  echo "Publishing artifacts to Maven repository..."

  # Make a temp directory for publish-snapshot.sh
  mkdir -p build/resources/publish/
  cp build/publish/publish-snapshot.sh build/resources/publish/
  chmod +x build/resources/publish/publish-snapshot.sh

  # Continue with the original flow
  cd build/resources/publish/
  cp -a $HOME/.m2/repository/* ./
  ./publish-snapshot.sh ./

  echo "Maven publishing completed"
}

# Function to update version metadata with commit ID
update_version_metadata() {
  local artifact_id="$1"
  local version="$2"
  local commit_id="$3"
  local snapshot_repo_url="${4:-$SNAPSHOT_REPO_URL}"

  if [ "$DISABLE_COMMIT_MAPPING" = "true" ]; then
    echo "Skipping version metadata update (commit mapping disabled)"
    return 0
  fi

  echo "Updating version metadata for ${artifact_id} version ${version} with commit ID ${commit_id}"

  TEMP_DIR=$(mktemp -d)
  METADATA_FILE="${TEMP_DIR}/maven-metadata.xml"

  # Download existing metadata
  META_URL="${snapshot_repo_url}org/opensearch/${artifact_id}/${version}/maven-metadata.xml"
  echo "Downloading metadata from ${META_URL}"

  if curl -s -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" -o "${METADATA_FILE}" "${META_URL}"; then
    if [ -s "${METADATA_FILE}" ]; then
      echo "Modifying metadata for ${version}"
      cp "${METADATA_FILE}" "${METADATA_FILE}.bak"

      # Add commit ID to metadata
      awk -v commit="${commit_id}" '
        /<versioning>/ {
          print $0
          print "  <commitId>" commit "</commitId>"
          next
        }
        {print}
      ' "${METADATA_FILE}.bak" > "${METADATA_FILE}"

      # Upload modified metadata
      echo "Uploading modified metadata to ${META_URL}"
      curl -X PUT -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" --upload-file "${METADATA_FILE}" "${META_URL}"

      # Update checksums
      cd "${TEMP_DIR}"
      sha256sum "maven-metadata.xml" | awk '{print $1}' > "maven-metadata.xml.sha256"
      sha512sum "maven-metadata.xml" | awk '{print $1}' > "maven-metadata.xml.sha512"

      # Upload checksums
      curl -X PUT -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" --upload-file "maven-metadata.xml.sha256" "${META_URL}.sha256"
      curl -X PUT -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" --upload-file "maven-metadata.xml.sha512" "${META_URL}.sha512"

      echo "Updated metadata and checksums for ${version}"
    else
      echo "Downloaded metadata file is empty for ${artifact_id}"
      return 1
    fi
  else
    echo "Failed to download metadata for ${artifact_id}"
    return 1
  fi

  # Clean up
  rm -rf "${TEMP_DIR}"

  echo "Version metadata updated successfully"
}

# Function to extract artifact version from metadata
extract_artifact_version() {
  local artifact_id="$1"
  local version="$2"
  local extension="$3"  # jar, zip, etc.
  local snapshot_repo_url="${4:-$SNAPSHOT_REPO_URL}"

  echo "Extracting ${extension} version for ${artifact_id} from metadata" >&2

  TEMP_METADATA=$(mktemp)
  META_URL="${snapshot_repo_url}org/opensearch/${artifact_id}/${version}/maven-metadata.xml"

  if curl -s -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" -o "${TEMP_METADATA}" "${META_URL}" && [ -s "${TEMP_METADATA}" ]; then
    # Extract the latest version for the specified extension
    ARTIFACT_VERSION=$(xmlstarlet sel -t -v "//snapshotVersion[extension='${extension}' and not(classifier)]/value" "${TEMP_METADATA}" | head -1)

    if [ -n "$ARTIFACT_VERSION" ]; then
      echo "Latest ${extension} version for ${artifact_id}: ${ARTIFACT_VERSION}" >&2
      echo "$ARTIFACT_VERSION"
    else
      echo "Warning: Could not find ${extension} version in metadata for ${artifact_id}" >&2
      echo "$version"
    fi
  else
    echo "Warning: Could not download or read metadata for ${artifact_id}" >&2
    echo "$version"
  fi

  rm -f "${TEMP_METADATA}"
}

# Function to update commit-version mapping
update_commit_mapping() {
  local commit_id="$1"
  local version="$2"
  local artifact_id="$3"
  local extension="$4"  # jar, zip, etc.
  local commit_map_filename="${5:-$COMMIT_MAP_FILENAME}"
  local snapshot_repo_url="${6:-$SNAPSHOT_REPO_URL}"

  if [ "$DISABLE_COMMIT_MAPPING" = "true" ]; then
    echo "Skipping commit-version mapping update (commit mapping disabled)"
    return 0
  fi

  echo "Updating commit-version mapping for ${artifact_id}"

  # Create temp directory for work
  MAPPING_DIR=$(mktemp -d)
  MAPPING_FILE="${MAPPING_DIR}/${commit_map_filename}"

  # Extract the actual artifact version from metadata
  ARTIFACT_VERSION=$(extract_artifact_version "$artifact_id" "$version" "$extension" "$snapshot_repo_url")

  # Try to download existing mapping file - MODIFIED: Changed URL structure
  MAPPING_URL="${snapshot_repo_url}org/opensearch/${artifact_id}/${commit_map_filename}"
  HTTP_CODE=$(curl -s -o "${MAPPING_FILE}" -w "%{http_code}" -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" "${MAPPING_URL}" || echo "000")

  if [ "$HTTP_CODE" = "200" ]; then
    echo "Downloaded existing mapping file"
  else
    echo "No existing mapping file found, creating new one"
    echo '{"mappings":[]}' > "${MAPPING_FILE}"
  fi

  # Add new mapping entry
  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

  # Use temporary file for JSON manipulation
  TEMP_JSON="${MAPPING_DIR}/temp.json"

  # Use jq to add the new mapping or update existing one
  # Pass values as strings instead of pre-built JSON to avoid escaping issues
  cat "${MAPPING_FILE}" | jq --arg commit "$commit_id" \
                            --arg timestamp "$TIMESTAMP" \
                            --arg artifact_id "$artifact_id" \
                            --arg base_version "$version" \
                            --arg artifact_version "$ARTIFACT_VERSION" '
  # Look for an existing entry with this commit ID
  if (.mappings | map(select(.commit_id == $commit)) | length) == 0 then
    # No entry exists, add a new one
    .mappings += [{"commit_id": $commit, "timestamp": $timestamp, "artifacts": {($artifact_id): {"base_version": $base_version, "artifact_version": $artifact_version}}}]
  else
    # Update the existing entry
    .mappings = [.mappings[] | if .commit_id == $commit then
      # Update timestamp and merge artifacts
      . + {"timestamp": $timestamp, "artifacts": (.artifacts + {($artifact_id): {"base_version": $base_version, "artifact_version": $artifact_version}})}
    else . end]
  end
  ' > "${TEMP_JSON}"

  mv "${TEMP_JSON}" "${MAPPING_FILE}"

  # Sort mappings by timestamp (newest first)
  cat "${MAPPING_FILE}" | jq '.mappings |= sort_by(.timestamp) | .mappings |= reverse' > "${TEMP_JSON}"
  mv "${TEMP_JSON}" "${MAPPING_FILE}"

  # Print the updated mapping
  echo "Updated mapping file content:"
  cat "${MAPPING_FILE}"

  # Upload the mapping file
  echo "Uploading mapping file to ${MAPPING_URL}"
  curl -v -u "${SONATYPE_USERNAME}:${SONATYPE_PASSWORD}" --upload-file "${MAPPING_FILE}" "${MAPPING_URL}"

  # Clean up
  rm -rf "${MAPPING_DIR}"

  echo "Commit mapping updated successfully"
}

# Function to create POM file
create_pom_file() {
  local group_id="$1"
  local artifact_id="$2"
  local version="$3"
  local packaging="${4:-jar}"
  local description="${5:-OpenSearch ${artifact_id}}"
  local output_path="$6"

  cat > "${output_path}" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>${group_id}</groupId>
    <artifactId>${artifact_id}</artifactId>
    <version>${version}</version>
    <packaging>${packaging}</packaging>
    <description>${description}</description>
</project>
EOF

  echo "POM file created at ${output_path}"
}

# Function to prepare Maven directory structure
prepare_maven_structure() {
  local group_id="$1"
  local artifact_id="$2"
  local version="$3"

  # Create directory structure in local Maven repository
  MAVEN_LOCAL_PATH="${HOME}/.m2/repository/${group_id//.//}/${artifact_id}/${version}"
  mkdir -p "${MAVEN_LOCAL_PATH}"

  echo "${MAVEN_LOCAL_PATH}"
}

# Main function for grammar files publishing workflow
publish_grammar_files() {
  local version="$1"
  local commit_id="$2"

  echo "Starting grammar files publishing workflow"

  # Define constants
  ARTIFACT_ID="language-grammar"
  GROUP_ID="org.opensearch"

  # Package grammar files
  echo "Packaging grammar files..."
  mkdir -p grammar_files
  find ./language-grammar/src/main/antlr4 -name "*.g4" -type f -exec cp {} grammar_files/ \;

  echo "Files to be included in the zip:"
  ls -la grammar_files/

  cd grammar_files
  zip -r ../grammar.zip ./*
  cd ..

  ls -la grammar.zip

  # Prepare for Maven publishing
  echo "Preparing for Maven publishing..."
  MAVEN_LOCAL_PATH=$(prepare_maven_structure "$GROUP_ID" "$ARTIFACT_ID" "$version")

  # Copy the zip file to Maven directory with proper naming
  MAVEN_ZIP_NAME="${ARTIFACT_ID}-${version}.zip"
  cp grammar.zip "${MAVEN_LOCAL_PATH}/${MAVEN_ZIP_NAME}"

  # Generate POM file
  create_pom_file "$GROUP_ID" "$ARTIFACT_ID" "$version" "zip" "OpenSearch Language Grammar Files" "${MAVEN_LOCAL_PATH}/${ARTIFACT_ID}-${version}.pom"

  echo "Grammar files prepared for Maven publishing as version ${version}"

  # Generate checksums
  generate_checksums

  # Publish to Maven
  publish_to_maven

  # Update metadata with commit ID
  update_version_metadata "$ARTIFACT_ID" "$version" "$commit_id"

  # Update commit mapping
  update_commit_mapping "$commit_id" "$version" "$ARTIFACT_ID" "zip"

  echo "Grammar files publishing workflow completed"
}

# Main function for async-query-core publishing workflow
publish_async_query_core() {
  local version="$1"
  local commit_id="$2"

  echo "Starting async-query-core publishing workflow"

  # Define constants
  ARTIFACT_ID="async-query-core"
  GROUP_ID="org.opensearch"

  # Build the shadow JAR
  echo "Building shadow JAR..."
  ./gradlew :async-query-core:shadowJar

  # Find the generated shadow JAR
  SHADOW_JAR=$(find ./async-query-core/build/libs/ -name "*-all.jar" | head -n 1)

  if [ -z "$SHADOW_JAR" ]; then
    echo "Error: Shadow JAR not found!"
    exit 1
  fi

  # Prepare Maven structure
  MAVEN_LOCAL_PATH=$(prepare_maven_structure "$GROUP_ID" "$ARTIFACT_ID" "$version")

  # Copy the shadow JAR to the local Maven repository with proper naming
  MAVEN_JAR_NAME="${ARTIFACT_ID}-${version}.jar"
  cp "${SHADOW_JAR}" "${MAVEN_LOCAL_PATH}/${MAVEN_JAR_NAME}"

  # Generate POM file
  create_pom_file "$GROUP_ID" "$ARTIFACT_ID" "$version" "jar" "OpenSearch Async Query Core" "${MAVEN_LOCAL_PATH}/${ARTIFACT_ID}-${version}.pom"

  echo "Shadow JAR and POM published to local Maven repository for version ${version}"

  # Generate checksums
  generate_checksums

  # Publish to Maven
  publish_to_maven

  # Update metadata with commit ID
  update_version_metadata "$ARTIFACT_ID" "$version" "$commit_id"

  # Update commit mapping
  update_commit_mapping "$commit_id" "$version" "$ARTIFACT_ID" "jar"

  echo "Async-query-core publishing workflow completed"
}