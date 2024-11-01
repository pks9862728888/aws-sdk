package com.demo.awssdkdemo.services;

import com.demo.awssdkdemo.configurations.AwsSdkConfigParams;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.datazone.DataZoneClient;
import software.amazon.awssdk.services.datazone.model.CreateAssetTypeRequest;
import software.amazon.awssdk.services.datazone.model.CreateAssetTypeResponse;
import software.amazon.awssdk.services.datazone.model.GetAssetTypeRequest;
import software.amazon.awssdk.services.datazone.model.GetAssetTypeResponse;
import software.amazon.awssdk.services.datazone.model.ListProjectsRequest;
import software.amazon.awssdk.services.datazone.model.ListProjectsResponse;
import software.amazon.awssdk.services.datazone.model.ResourceNotFoundException;

@Service
@Slf4j
@RequiredArgsConstructor
public class AwsDatazoneService {
    private final DataZoneClient dataZoneClient;
    private final AwsSdkConfigParams awsSdkConfigParams;

    public void listProjects() {
        log.info("Listing projects...");
        ListProjectsResponse listProjectsResponse = dataZoneClient.listProjects(ListProjectsRequest.builder()
                .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
                .build());
        log.info("List Project response: {}", listProjectsResponse.toString());
    }

    public void createAssetType(String assetType) {
        log.info("Creating asset type...");
        if (!assetTypeExists(assetType)) {
            CreateAssetTypeResponse createAssetTypeRes = dataZoneClient.createAssetType(CreateAssetTypeRequest
                    .builder()
                    .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
//                    .owningProjectIdentifier(awsSdkConfigParams.getProjectIdentifier())
                    .build());
            log.info("AssetType created: {}", createAssetTypeRes);
        } else {
            log.info("Asset type already exists: {}", assetType);
        }
    }

    public boolean assetTypeExists(String assetType) {
        try {
            log.info("Checking if asset type exists: {}", assetType);
            GetAssetTypeResponse assetTypeResponse = dataZoneClient.getAssetType(GetAssetTypeRequest.builder()
                    .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
                    .identifier(assetType)
                    .build());
            log.info("GetAssetType response: {}", assetTypeResponse);
            return true;
        } catch (ResourceNotFoundException r) {
            return false;
        }
    }
}
