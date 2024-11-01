package com.demo.awssdkdemo.configurations;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.regions.Region;

@Configuration
@Getter
@Setter
@ConfigurationProperties(prefix = "aws")
public class AwsSdkConfigParams {
    @Getter(AccessLevel.NONE)
    private String region;
    private String domainIdentifier;

    public Region getRegion() {
        return Region.of(region);
    }
}
