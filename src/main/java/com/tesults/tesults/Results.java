package com.tesults.tesults;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.transfer.Transfer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.JSONObject;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.auth.*;


public class Results {
    final static String tesultsUrl = "https://www.tesults.com";
    final static long expireBuffer = 30; // 30 seconds

    private static class RefreshCredentialsResponse {
        public Boolean success;
        public String message;
        public JSONObject upload;
    }

    private static class FileUploadReturn {
        public String message;
        public List<String> warnings;
    }

    private static class ResultsFile {
        public int num;
        public File file;
    }

    private static RefreshCredentialsResponse refreshCredentialsError (Boolean success, String message) {
        RefreshCredentialsResponse response = new RefreshCredentialsResponse();
        response.success = success;
        response.message = message;
        response.upload = new JSONObject();
        return response;
    }

    private static RefreshCredentialsResponse RefreshCredentials (String target, String key) {
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("target", target);
        data.put("key", key);
        String jsonData;
        try {
            jsonData = new ObjectMapper().writeValueAsString(data);
        } catch (JsonProcessingException ex) {
            return refreshCredentialsError(false, "Incorrect data format (1).");
        }

        URL url;
        try {
            url = new URL(tesultsUrl + "/permitupload");
        } catch (MalformedURLException ex) {
            return refreshCredentialsError(false, "Incorrect url.");
        }

        HttpURLConnection connection;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
        } catch (IOException ex) {
            return refreshCredentialsError(false, "Unable to connect.");
        }

        connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        try {
            connection.setRequestMethod("POST");
        } catch (ProtocolException ex) {
            return refreshCredentialsError(false, "Bad request method.");
        }

        OutputStream os;
        try {
            os = connection.getOutputStream();
        } catch (IOException ex) {
            //System.out.println(ex.getMessage());
            return refreshCredentialsError(false, "Bad response.");
        }

        try {
            os.write(jsonData.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            return refreshCredentialsError(false, "Bad encoding.");
        } catch (IOException ex) {
            return refreshCredentialsError(false, "Unable to write data.");
        }

        try {
            os.close();
        } catch (IOException ex) {
            return refreshCredentialsError(false, "Unable to close connection.");
        }

        // read the response
        InputStream in;
        JSONObject jsonObject;
        boolean success = false;
        try {
            if (connection.getResponseCode() == 200) {
                success = true;
                in = new BufferedInputStream(connection.getInputStream());
            } else {
                in = new BufferedInputStream(connection.getErrorStream());
            }
            String result = org.apache.commons.io.IOUtils.toString(in, "UTF-8");
            jsonObject = new JSONObject(result);
            in.close();
        } catch (IOException ex) {
            return refreshCredentialsError(false, "Error processing response.");
        }

        if (success != true) {
            String message = (String) jsonObject.getJSONObject("error").get("message");
            connection.disconnect();
            return refreshCredentialsError(false, message);
        }

        // success
        JSONObject successData = jsonObject.getJSONObject(("data"));

        RefreshCredentialsResponse response = new RefreshCredentialsResponse();
        response.success = true;
        response.message = (String) successData.get("message");
        response.upload = successData.getJSONObject("upload");
        return response;
    }

    private static TransferManager CreateTransferManager(JSONObject auth) {
        String accessKeyId = (String) auth.get("AccessKeyId");
        String secretAccessKey = (String) auth.get("SecretAccessKey");
        String sessionToken = (String) auth.get("SessionToken");

        BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
                accessKeyId,
                secretAccessKey,
                sessionToken);

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(new AWSStaticCredentialsProvider(sessionCredentials)).build();
        TransferManager transferManager = TransferManagerBuilder.standard().withS3Client(s3).build();
        return transferManager;
    }


    private static FileUploadReturn filesUpload (List<ResultsFile> files, String keyPrefix, JSONObject auth, String target) {
        int filesUploaded = 0;
        long bytesUploaded = 0;
        long expiration = auth.getLong("Expiration");

        List<String> warnings = new ArrayList<String>();
        List<Transfer> uploading = new ArrayList<Transfer>();
        final int maxActiveUploads = 10; // Upload at most 10 files simultaneously to avoid hogging the client machine.
        TransferManager transferManager = CreateTransferManager(auth);

        while (!files.isEmpty() || !uploading.isEmpty()) {
            try {
                if (uploading.size() < maxActiveUploads && !files.isEmpty()) {

                    // check if new credentials required
                    long now = System.currentTimeMillis() / 1000L;
                    if (now + expireBuffer > expiration) { // check within  30 seconds of expiry
                        if (uploading.isEmpty()) {
                            // wait for all current transfers to complete so we can set a new transfer manager
                            RefreshCredentialsResponse response = RefreshCredentials(target, keyPrefix);
                            if (response.success != true) {
                                // Must stop upload due to failure to get new credentials.
                                warnings.add(response.message);
                                break;
                            } else {
                                String key = (String) response.upload.get("key");
                                String uploadMessage = (String) response.upload.get("message");
                                Boolean permit = (Boolean) response.upload.get("permit");
                                auth = response.upload.getJSONObject("auth");

                                if (permit != true) {
                                    // Must stop upload due to failure to be permitted for new credentials.
                                    warnings.add(uploadMessage);
                                    break;
                                }

                                // upload permitted
                                expiration = auth.getLong("Expiration");
                                transferManager = CreateTransferManager(auth);
                            }
                        }
                    }

                    if (now + expireBuffer < expiration) { // check within 30 seconds of expiry
                        // load new file for upload
                        ResultsFile resultsFile = files.remove(0);
                        if (!resultsFile.file.exists() || resultsFile.file.isDirectory()) {
                            warnings.add("File not found: " + resultsFile.file.getName());
                        } else {
                            String key = keyPrefix + "/" + resultsFile.num + "/" + resultsFile.file.getName();
                            uploading.add(transferManager.upload("tesults-results", key, resultsFile.file));
                        }
                    }
                }

                // check existing uploads complete
                for (Iterator<Transfer> iterator = uploading.iterator(); iterator.hasNext(); ) {
                    Transfer transfer = iterator.next();
                    if (transfer.isDone()) {
                        bytesUploaded += transfer.getProgress().getBytesTransferred();
                        filesUploaded += 1;
                        iterator.remove();
                    }
                }
            }
            catch (AmazonServiceException e){
                warnings.add(e.getErrorMessage());
                warnings.add(e.getMessage());
                warnings.add("Failed to upload file.");
            }
            catch (AmazonClientException e) {
                warnings.add("Failed to upload file.");
            }
            catch (Exception e) {
                warnings.add("Failed to upload file.");
            }
        }

        transferManager.shutdownNow();
        FileUploadReturn fileUploadReturn = new FileUploadReturn();
        fileUploadReturn.message = "Success. " + filesUploaded + " files uploaded. " + bytesUploaded + " bytes uploaded.";
        fileUploadReturn.warnings = warnings;
        return fileUploadReturn;
    }

    private static List<ResultsFile> filesInTestCases (Map<String, Object> data) {
        // file upload required, build the list
        @SuppressWarnings("unchecked")
        Map<String, Object> results = (Map<String, Object>) data.get("results");
        @SuppressWarnings("unchecked")
        List<Map<String,Object>> cases = (List<Map<String,Object>>) results.get("cases");
        List<ResultsFile> files = new ArrayList<ResultsFile>();
        int num = 0;
        for (Map<String, Object> c : cases) {
            if (c.containsKey("files")) {
                @SuppressWarnings("unchecked")
                List<String> caseFiles = (List<String>) c.get("files");
                for (String caseFile : caseFiles) {
                    ResultsFile resultsFile = new ResultsFile();
                    resultsFile.num = num;
                    resultsFile.file = new File(caseFile);
                    files.add(resultsFile);
                }
            }
            num++;
        }
        return files;
    }

    private static Map<String, Object> uploadResult (Boolean success, String message, List<String> warnings) {
        Map<String, Object> uploadResult = new HashMap<String, Object>();
        List<String> errors = new ArrayList<String>();
        uploadResult.put("success", success);
        uploadResult.put("message", message);
        if (success != true) {
            errors.add(message);
        }
        uploadResult.put("warnings", warnings);
        uploadResult.put("errors", errors);
        return uploadResult;
    }

    public static Map<String, Object> upload (Map<String, Object> data) {
        List<String> warnings = new ArrayList<String>();
        String jsonData;
        try {
            jsonData = new ObjectMapper().writeValueAsString(data);
        } catch (JsonProcessingException ex) {
            return uploadResult(false, "Incorrect data format (2).", warnings);
        }

        URL url;
        try {
            url = new URL(tesultsUrl + "/results");
        } catch (MalformedURLException ex) {
            return uploadResult(false, "Incorrect url.", warnings);
        }

        HttpURLConnection connection;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
        } catch (IOException ex) {
            return uploadResult(false, "Unable to connect.", warnings);
        }

        connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        try {
            connection.setRequestMethod("POST");
        } catch (ProtocolException ex) {
            return uploadResult(false, "Bad request method.", warnings);
        }

        OutputStream os;
        try {
            os = connection.getOutputStream();
        } catch (IOException ex) {
            //System.out.println(ex.getMessage());
            return uploadResult(false, "Bad response.", warnings);
        }

        try {
            os.write(jsonData.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            return uploadResult(false, "Bad encoding.", warnings);
        } catch (IOException ex) {
            return uploadResult(false, "Unable to write data.", warnings);
        }

        try {
            os.close();
        } catch (IOException ex) {
            return uploadResult(false, "Unable to close connection.", warnings);
        }

        // read the response
        InputStream in;
        JSONObject jsonObject;
        boolean success = false;
        try {
            if (connection.getResponseCode() == 200) {
                success = true;
                in = new BufferedInputStream(connection.getInputStream());
            } else {
                in = new BufferedInputStream(connection.getErrorStream());
            }
            String result = org.apache.commons.io.IOUtils.toString(in, "UTF-8");
            jsonObject = new JSONObject(result);
            in.close();
        } catch (IOException ex) {
            return uploadResult(false, "Error processing response.", warnings);
        }

        if (success != true) {
            String message = (String) jsonObject.getJSONObject("error").get("message");
            connection.disconnect();
            return uploadResult(false, message, warnings);
        }

        // success
        JSONObject successData = jsonObject.getJSONObject(("data"));
        String message = (String) successData.get("message");

        if (successData.has("upload")) {
            String target = (String) data.get("target");
            List<ResultsFile> files = filesInTestCases(data);

            JSONObject upload = successData.getJSONObject("upload");
            String key = (String) upload.get("key");
            String uploadMessage = (String) upload.get("message");
            Boolean permit = (Boolean) upload.get("permit");
            JSONObject auth = upload.getJSONObject("auth");
            if (permit != true) {
                warnings.add(uploadMessage);
                connection.disconnect();
                return uploadResult(true, message, warnings);
            }

            // upload required and permitted
            FileUploadReturn fileUploadReturn = filesUpload(files, key, auth, target); // This can take a while
            connection.disconnect();
            return uploadResult(true, fileUploadReturn.message, fileUploadReturn.warnings);
        } else {
            // upload not required
            return uploadResult(true, message, warnings);
        }
    }
}