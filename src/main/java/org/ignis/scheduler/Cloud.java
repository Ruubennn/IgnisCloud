package org.ignis.scheduler;

import org.ignis.scheduler.IScheduler;
import org.ignis.scheduler.ISchedulerException;
import org.ignis.scheduler.ISchedulerUtils;
import org.ignis.scheduler.model.IClusterInfo;
import org.ignis.scheduler.model.IClusterRequest;
import org.ignis.scheduler.model.IContainerInfo;
import org.ignis.scheduler.model.IJobInfo;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.services.ec2.model.ResourceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.TagSpecification;
import software.amazon.awssdk.services.ec2.model.ResourceType;

import java.net.URI; // Necesario para LocalStack

public class Cloud implements IScheduler {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Cloud.class);
    /*private static final String SUBNET_ID = "subnet-97e34da82877256a3";
    private static final String SECURITY_GROUP_ID = "sg-1800963b8c7b84d1f";
    private static final String AMI_ID = "ami-df570af1";*/

    /*private final String url;*/
    //private final String subnetId;
    //private final String securityGroupId;
    /*private final Ec2Client ec2Client;*/

    public Cloud(String url) throws ISchedulerException{
        /*this.url = url;
        this.ec2Client = Ec2Client.builder()
                .endpointOverride(URI.create(url))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();

        System.out.println("\n*****************************************");
        System.out.println("¡CONECTADO A LOCALSTACK!");
        System.out.println("Subnet: " + SUBNET_ID);
        System.out.println("SG: " + SECURITY_GROUP_ID);
        System.out.println("*****************************************\n");*/
        System.out.println("\n*****************************************");
        System.out.println("URL: " + url);
        System.out.println("*****************************************\n");

        LOGGER.info("Initializing Cloud scheduler at: " + url);

        runTerraform();
    }

    private void runTerraform()  throws ISchedulerException {
        String terraformDir  = "/home/ruben/TFG/ignis-project/core-base/scheduler-cloud/terraform";
        LOGGER.info("Starting infrastructure provisioning via Terraform...");

        try {
            executeCommand(terraformDir, "/usr/bin/terraform", "init");
            executeCommand(terraformDir, "/usr/bin/terraform", "apply", "-auto-approve");
            LOGGER.info("Terraform infrastructure applied successfully.");
        } catch (ISchedulerException e){
            LOGGER.error("Failed to provision infrastructure: " + e.getMessage());
            throw e;
        }
    }

   /* private void executeCommand(String dir, String... command) throws ISchedulerException {
        try {
            String fullCommand = String.join(" ", command);
            LOGGER.info("Executing command: " + fullCommand);

            ProcessBuilder pb = new ProcessBuilder(command);
            pb.directory(new java.io.File(dir));
            pb.inheritIO();

            Process process = pb.start();
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new ISchedulerException("External command failed with exit code " + exitCode + ": " + command[0]);
            }
        } catch (IOException e) {
            throw new ISchedulerException("I/O error while executing Terraform. Is it installed?", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Terraform execution was unexpectedly interrupted.", e);
        }
    }*/

    private void executeCommand(String dir, String... command) throws ISchedulerException {
        try {
            String fullCommand = String.join(" ", command);
            LOGGER.info("Executing command: " + fullCommand + " in directory: " + dir);

            ProcessBuilder pb = new ProcessBuilder(command);
            pb.directory(new File(dir));

            // ¡Muy importante! Redirigir error y salida para capturar el mensaje real
            pb.redirectErrorStream(true);
            Process process = pb.start();

            // Leer salida/error en tiempo real (opcional pero útil)
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[Terraform out] " + line);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new ISchedulerException("Command failed with exit code " + exitCode + ": " + fullCommand);
            }
        } catch (IOException e) {
            // ¡Imprime el stacktrace completo y la causa!
            e.printStackTrace();
            LOGGER.error("Real IOException message: " + e.getMessage(), e);
            throw new ISchedulerException("I/O error executing: " + String.join(" ", command) + " → " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ISchedulerException("Interrupted", e);
        }
    }


    /*private String runEc2Instance(String jobName, IClusterRequest request) throws ISchedulerException {
        try {
            // 1. Preparamos la etiqueta con el nombre (equivalente al bloque 'tags' de tu Terraform)
            Tag nameTag = Tag.builder()
                    .key("Name")
                    .value(jobName)
                    .build();

            TagSpecification tagSpecification = TagSpecification.builder()
                    .resourceType(ResourceType.INSTANCE)
                    .tags(nameTag)
                    .build();

            // 2. Creamos la petición de lanzamiento (equivalente a tu resource "aws_instance")
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(AMI_ID)
                    // Usamos el Enum correspondiente en lugar de un String a machete
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(1)
                    .minCount(1)
                    .subnetId(SUBNET_ID)
                    .securityGroupIds(SECURITY_GROUP_ID)
                    .tagSpecifications(tagSpecification)
                    .build();

            // 3. Ejecutamos la orden en LocalStack
            RunInstancesResponse response = ec2Client.runInstances(runRequest);

            // 4. Obtenemos el ID de la instancia creada
            String instanceId = response.instances().get(0).instanceId();
            System.out.println("[AWS-SDK] Instancia desplegada con éxito. ID: " + instanceId);

            return instanceId;

        } catch (Exception e) {
            LOGGER.error("Error al interactuar con el SDK de AWS", e);
            throw new ISchedulerException("No se pudo levantar la instancia en LocalStack: " + e.getMessage());
        }
    }*/

    @Override
    public String createJob(String name, IClusterRequest driver, IClusterRequest... executors) throws ISchedulerException {
        // 1. Generamos el ID del Job como hace Docker.java
        String jobId = ISchedulerUtils.genId().substring(0, 8);
        String finalJobName = name.replace("/", "-") + "-" + jobId;

        System.out.println("[CLOUD] Lanzando instancia para el Driver: " + finalJobName);

        try {
            // Aquí llamaremos al método que creará la instancia en AWS
            /*String instanceId = runEc2Instance(finalJobName, driver);*/

            // Por ahora retornamos el ID para que RunJob sea feliz
            return finalJobName;
        } catch (Exception e) {
            e.printStackTrace(); // <--- AÑADE ESTO para ver el error real en la consola
            LOGGER.error("Error al interactuar con el SDK de AWS", e);
            throw new ISchedulerException("No se pudo levantar la instancia en LocalStack: " + e.getMessage());

        }
    }

    @Override
    public void cancelJob(String id) throws ISchedulerException {
    }

    @Override
    public IJobInfo getJob(String id) throws ISchedulerException {
        return null;
    }

    @Override
    public List<IJobInfo> listJobs(Map<String, String> filters) throws ISchedulerException {
        return null;
    }

    @Override
    public IClusterInfo createCluster(String job, IClusterRequest request) throws ISchedulerException {
        return null;
    }

    @Override
    public void destroyCluster(String job, String id) throws ISchedulerException {
    }

    @Override
    public IClusterInfo getCluster(String job, String id) throws ISchedulerException {
        return null;
    }

    @Override
    public IClusterInfo repairCluster(String job, IClusterInfo cluster, IClusterRequest request) throws ISchedulerException {
        return null;
    }

    @Override
    public IContainerInfo.IStatus getContainerStatus(String job, String id) throws ISchedulerException {
       return null;
    }

    @Override
    public void healthCheck() throws ISchedulerException {
        try {
            // Si esto responde sin error, la conexión con LocalStack está viva
            //ec2Client.describeAvailabilityZones();
        } catch (Exception ex) {
            throw new ISchedulerException("LocalStack no responde: " + ex.getMessage(), ex);
        }
    }


}




