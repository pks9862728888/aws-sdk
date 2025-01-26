package com.demo.awssdkdemo.services;

import com.demo.awssdkdemo.configurations.AwsSdkConfigParams;
import com.demo.awssdkdemo.serializer.CustomZonedDateTimeSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.openlineage.client.OpenLineage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.datazone.DataZoneClient;
import software.amazon.awssdk.services.datazone.model.CreateAssetRequest;
import software.amazon.awssdk.services.datazone.model.CreateAssetResponse;
import software.amazon.awssdk.services.datazone.model.CreateAssetRevisionRequest;
import software.amazon.awssdk.services.datazone.model.CreateAssetRevisionResponse;
import software.amazon.awssdk.services.datazone.model.FormInput;
import software.amazon.awssdk.services.datazone.model.GetAssetRequest;
import software.amazon.awssdk.services.datazone.model.GetAssetResponse;
import software.amazon.awssdk.services.datazone.model.GetAssetTypeRequest;
import software.amazon.awssdk.services.datazone.model.GetAssetTypeResponse;
import software.amazon.awssdk.services.datazone.model.GetLineageEventRequest;
import software.amazon.awssdk.services.datazone.model.GetLineageEventResponse;
import software.amazon.awssdk.services.datazone.model.ListLineageEventsRequest;
import software.amazon.awssdk.services.datazone.model.ListLineageEventsResponse;
import software.amazon.awssdk.services.datazone.model.ListProjectsRequest;
import software.amazon.awssdk.services.datazone.model.ListProjectsResponse;
import software.amazon.awssdk.services.datazone.model.PostLineageEventRequest;
import software.amazon.awssdk.services.datazone.model.PostLineageEventResponse;
import software.amazon.awssdk.services.datazone.model.ProjectSummary;
import software.amazon.awssdk.services.datazone.model.ResourceNotFoundException;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class AwsDatazoneService {
  private final DataZoneClient dataZoneClient;
  private final AwsSdkConfigParams awsSdkConfigParams;

  public ListProjectsResponse listProjects() {
    log.info("Listing projects...");
    ListProjectsResponse listProjectsResponse = dataZoneClient.listProjects(ListProjectsRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .build());
    log.info("List Project response: {}", listProjectsResponse.toString());
    return listProjectsResponse;
  }

  public Optional<String> getProjectId(String projectName) {
    log.info("GetProject id: {}", projectName);
    return listProjects()
        .items()
        .stream()
        .filter(p -> p.name().equalsIgnoreCase(projectName))
        .map(ProjectSummary::id)
        .findFirst();
  }

  public void getAsset(String assetId) {
    log.info("Finding asset by assetId: {}", assetId);
    GetAssetResponse getAssetResponse = dataZoneClient.getAsset(GetAssetRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .identifier(assetId)
        .build());
    log.info("GetAsset response: {}", getAssetResponse);
  }

  public void createAsset(
      String assetName, String assetType, String owningProject, List<String> glossaryTerms,
      List<FormInput> metaDataForms) {
    String owningProjectId = getProjectId(owningProject)
        .orElseThrow(() -> new RuntimeException("Project not found: " + owningProject));
    String assetTypeId = getAssetTypeId(assetType)
        .orElseThrow(() -> new RuntimeException("Asset type not found: " + assetType));
    log.info("Creating asset...");
    CreateAssetRequest createAssetRequest = CreateAssetRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .name(assetName)
        .owningProjectIdentifier(owningProjectId)
        .description("Test asset creation: " + assetName)
        .formsInput(metaDataForms)
        .typeIdentifier(assetTypeId)
        .build();
    if (!glossaryTerms.isEmpty()) {
      createAssetRequest = createAssetRequest.toBuilder()
          .glossaryTerms(glossaryTerms)
          .build();
    }
    CreateAssetResponse createAssetResponse = dataZoneClient.createAsset(createAssetRequest);
    log.info("CreateAsset response: {}", createAssetResponse);
  }

  public void updateAsset(String assetName, String assetId, List<FormInput> metaDataForms) {
    // This is POST request (i.e updates everything)
    log.info("Updating asset: {}", assetName);
    CreateAssetRevisionRequest createAssetRevision = CreateAssetRevisionRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .name(assetName)
        .description("Test asset creation: " + assetName)
        .identifier(assetId)
        .formsInput(metaDataForms)
        .build();
    CreateAssetRevisionResponse createAssetRevisionRequest =
        dataZoneClient.createAssetRevision(createAssetRevision);
    log.info("CreateAssetRevision response: {}", createAssetRevisionRequest);
  }

  public void postLineageEvent(
      String sourceAssetId, String targetAssetId, String sourceDatasetName, String targetDataSetName) throws JsonProcessingException {
    log.info("Posting lineage event: {} -> {}", sourceAssetId, targetAssetId);
    OpenLineage ol = new OpenLineage(URI.create(""));
    OpenLineage.RunEvent openLineageRunEvent = ol.newRunEventBuilder()
        .job(ol.newJobBuilder()
            .name("DatazoneLineageJob" + UUID.randomUUID())
            .namespace(awsSdkConfigParams.getDomainIdentifier())
            .facets(ol.newJobFacetsBuilder()
                .jobType(ol.newJobTypeJobFacetBuilder()
                    .jobType("JOB")
                    .processingType("STREAMING")
                    .integration("SPARK")
                    .build())
                .build())
            .build())
        .run(ol.newRunBuilder()
            .runId(UUID.randomUUID())
            .facets(ol.newRunFacetsBuilder()
                .nominalTime(ol.newNominalTimeRunFacetBuilder()
                    .nominalStartTime(ZonedDateTime.now().minusHours(1))
                    .nominalEndTime(ZonedDateTime.now())
                    .build())
                .build())
            .build())
        .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
        .eventTime(ZonedDateTime.now())
        .inputs(List.of(ol.newInputDataset(awsSdkConfigParams.getDomainIdentifier(),
                    sourceAssetId,
                    ol.newDatasetFacetsBuilder()
                        .schema(ol.newSchemaDatasetFacetBuilder()
                            .fields(List.of(
                                ol.newSchemaDatasetFacetFields("departmentid", "bigint", "id desc", Collections.emptyList()),
                                ol.newSchemaDatasetFacetFields("name", "string", "name desc", Collections.emptyList()),
                                ol.newSchemaDatasetFacetFields("manager", "string", "manager desc", Collections.emptyList()),
                                ol.newSchemaDatasetFacetFields("employees", "int", "employees desc", Collections.emptyList()),
                                ol.newSchemaDatasetFacetFields("partition_id", "string", "partition_id desc", Collections.emptyList())
                            ))
                            .build())
//                        .columnLineage(ol.newColumnLineageDatasetFacetBuilder()
//                            .fields(ol.newColumnLineageDatasetFacetFieldsBuilder()
//                                .put("departmentid", ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
//                                    .transformationType("IDENTITY")
//                                    .transformationDescription("Direct mapping")
//                                    .build())
//                                .build())
//                            .build())
                        .build(),
                    null
                )
            )
        )
        .outputs(List.of(ol.newOutputDataset(awsSdkConfigParams.getDomainIdentifier(),
            targetAssetId,
            ol.newDatasetFacetsBuilder()
                .schema(ol.newSchemaDatasetFacetBuilder()
                    .fields(List.of(
                        ol.newSchemaDatasetFacetFields("userid", "bigint", "id desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("firstname", "string", "firstname desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("lastname", "string", "lastname desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("email", "string", "email desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("age", "int", "age desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("departmentid", "bigint", "departmentid desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("name", "string", "name desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("manager", "string", "manager desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("employees", "int", "employees desc", Collections.emptyList()),
                        ol.newSchemaDatasetFacetFields("partition_id", "string", "partition_id desc", Collections.emptyList())
                    ))
                    .build())
                .columnLineage(ol.newColumnLineageDatasetFacetBuilder()
                    .fields(ol.newColumnLineageDatasetFacetFieldsBuilder()
                        .put("departmentid", ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                            .transformationType("IDENTITY")
                            .transformationDescription("Direct mapping")
                            .inputFields(List.of(ol.newInputField(
                                awsSdkConfigParams.getDomainIdentifier(),
                                sourceDatasetName,
                                "departmentid",
                                List.of(ol.newInputFieldTransformationsBuilder()
                                    .masking(false)
                                    .description("Direct mapping")
                                    .type("DIRECT")
                                    .build()))))
                            .build())
                        .put("name", ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                            .transformationType("IDENTITY")
                            .transformationDescription("Direct mapping")
                            .inputFields(List.of(ol.newInputField(
                                awsSdkConfigParams.getDomainIdentifier(),
                                sourceDatasetName,
                                "name",
                                List.of(ol.newInputFieldTransformationsBuilder()
                                    .masking(false)
                                    .description("Direct mapping")
                                    .type("DIRECT")
                                    .build()))))
                            .build())
                        .put("manager", ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                            .transformationType("IDENTITY")
                            .transformationDescription("Direct mapping")
                            .inputFields(List.of(ol.newInputField(
                                awsSdkConfigParams.getDomainIdentifier(),
                                sourceDatasetName,
                                "manager",
                                List.of(ol.newInputFieldTransformationsBuilder()
                                    .masking(false)
                                    .description("Direct mapping")
                                    .type("DIRECT")
                                    .build()))))
                            .build())
                        .put("employees", ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                            .transformationType("IDENTITY")
                            .transformationDescription("Direct mapping")
                            .inputFields(List.of(ol.newInputField(
                                awsSdkConfigParams.getDomainIdentifier(),
                                sourceDatasetName,
                                "employees",
                                List.of(ol.newInputFieldTransformationsBuilder()
                                    .masking(false)
                                    .description("Direct mapping")
                                    .type("DIRECT")
                                    .build()))))
                            .build())
                        .build())
                    .build())
                .build(),
            ol.newOutputDatasetOutputFacetsBuilder()
                .put("nameAlias", new OpenLineage.DefaultOutputDatasetFacet(URI.create("")))
                .build())))
        .build();

    ObjectMapper objectMapper = getObjectMapper();
    String runEvent = objectMapper.writeValueAsString(openLineageRunEvent);
    log.info("{}", runEvent);

    // Post lineage event
    PostLineageEventRequest postLineageEventRequest = PostLineageEventRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .clientToken(UUID.randomUUID().toString())
        .event(SdkBytes.fromUtf8String(runEvent))
        .build();
    PostLineageEventResponse postLineageEventResponse = dataZoneClient.postLineageEvent(postLineageEventRequest);
    log.info("PostLineageEvent response: {}", postLineageEventResponse);
  }

  private static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    JavaTimeModule javaTimeModule = new JavaTimeModule();
    // Add a LocalDateTimeDeserializer with the ISO-8601 format
    javaTimeModule.addSerializer(ZonedDateTime.class, new CustomZonedDateTimeSerializer());
    objectMapper.registerModule(javaTimeModule);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    return objectMapper;
  }

  private Optional<String> getAssetTypeId(String assetType) {
    log.info("Finding asset type: {}", assetType);
    GetAssetTypeResponse assetTypeResponse = dataZoneClient.getAssetType(GetAssetTypeRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .identifier(assetType)
        .build());
    log.info("GetAssetType response: {}", assetTypeResponse);
    return Optional.ofNullable(assetTypeResponse.name());
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

  public void getLineageEvent(String lineageEventId) {
    // Need to know lineage event id
    GetLineageEventResponse getLineageEvent = dataZoneClient.getLineageEvent(GetLineageEventRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .identifier(lineageEventId)
        .build());
    log.info("GetLinageEvent response: {}", getLineageEvent);
  }

  public void listLineageEvents() {
    log.info("Listing lineage events...");
    ListLineageEventsResponse listLineageEvents = dataZoneClient.listLineageEvents(ListLineageEventsRequest.builder()
        .domainIdentifier(awsSdkConfigParams.getDomainIdentifier())
        .build());
    log.info("ListLineageEvents response: {}", listLineageEvents);
  }
}
