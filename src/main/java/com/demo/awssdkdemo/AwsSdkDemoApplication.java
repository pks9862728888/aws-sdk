package com.demo.awssdkdemo;

import com.demo.awssdkdemo.services.AwsDatazoneService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class AwsSdkDemoApplication implements CommandLineRunner {
    private final AwsDatazoneService awsDatazoneService;

    public static void main(String[] args) {
//        System.setProperty(SdkSystemSetting.AWS_DISA)
        SpringApplication.run(AwsSdkDemoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
//        awsDatazoneService.listProjects();
        awsDatazoneService.assetTypeExists("testasset1");
        System.exit(1);
    }
}
