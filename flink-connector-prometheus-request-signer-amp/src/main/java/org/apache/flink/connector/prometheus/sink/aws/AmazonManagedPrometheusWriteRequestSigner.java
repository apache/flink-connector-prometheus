package org.apache.flink.connector.prometheus.sink.aws;

import org.apache.flink.connector.prometheus.sink.PrometheusRequestSigner;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

/** Sign a Remote-Write request to Amazon Managed Service for Prometheus (AMP). */
public class AmazonManagedPrometheusWriteRequestSigner implements PrometheusRequestSigner {
    // Header names
    private static final String X_AMZ_CONTENT_SHA_256 = "x-amz-content-sha256";
    private static final String AUTHORIZATION = "Authorization";

    private final URL remoteWriteUrl;
    private final String awsRegion;

    /**
     * Creates a signer instance using DefaultAWSCredentialsProviderChain.
     *
     * @param remoteWriteUrl URL of the remote-write endpoint
     * @param awsRegion Region of the AMP workspace
     */
    public AmazonManagedPrometheusWriteRequestSigner(String remoteWriteUrl, String awsRegion) {
        Preconditions.checkArgument(
                StringUtils.isNotBlank(awsRegion), "Missing or blank AMP workspace region");
        Preconditions.checkNotNull(
                StringUtils.isNotBlank(remoteWriteUrl),
                "Missing or blank AMP workspace remote-write URL");
        this.awsRegion = awsRegion;
        try {
            this.remoteWriteUrl = new URL(remoteWriteUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(
                    "Invalid AMP remote-write URL: " + remoteWriteUrl, e);
        }
    }

    /**
     * Add the additional Http request headers required by Amazon Managed Prometheus:
     * 'x-amz-content-sha256', 'Host', 'X-Amz-Date', 'x-amz-security-token' and 'Authorization`.
     *
     * @param requestHeaders original Http request headers. It must be mutable. For efficiency, any
     *     new header is added to the map, instead of making a copy.
     * @param requestBody request body, already compressed
     */
    @Override
    public void addSignatureHeaders(Map<String, String> requestHeaders, byte[] requestBody) {
        byte[] contentHash = AWS4SignerForAMP.hash(requestBody);
        String contentHashString = AWS4SignerForAMP.toHex(contentHash);

        // x-amz-content-sha256 must be included before generating the Authorization header
        requestHeaders.put(X_AMZ_CONTENT_SHA_256, contentHashString);

        // Get the credentials from the default credential provider chain
        AwsCredentialsProvider awsCredProvider = DefaultCredentialsProvider.create();
        AwsCredentials awsCreds = awsCredProvider.resolveCredentials();

        // If the credentials are from a session, also get the session token
        String sessionToken =
                (awsCreds instanceof AwsSessionCredentials)
                        ? ((AwsSessionCredentials) awsCreds).sessionToken()
                        : null;

        AWS4SignerForAMP signer = new AWS4SignerForAMP(remoteWriteUrl, awsRegion);

        // computeSignature also adds 'Host', 'X-Amz-Date' and 'x-amz-security-token' to the
        // requestHeaders Map
        String authorization =
                signer.computeSignature(
                        requestHeaders,
                        null, // no query parameters
                        contentHashString,
                        awsCreds.accessKeyId(),
                        awsCreds.secretAccessKey(),
                        sessionToken);
        requestHeaders.put(AUTHORIZATION, authorization);
    }
}
