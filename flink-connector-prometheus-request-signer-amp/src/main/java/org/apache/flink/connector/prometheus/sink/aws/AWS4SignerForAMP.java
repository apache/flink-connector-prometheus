/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.prometheus.sink.aws;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Implements Http/1.1 AWS Signature Version 4a request signing for POST requests to the Amazon
 * Managed Prometheus service (`aps`).
 */
@Internal
public class AWS4SignerForAMP {
    private static final String SCHEME = "AWS4";
    private static final String ALGORITHM = "HMAC-SHA256";
    private static final String TERMINATOR = "aws4_request";
    private static final String HTTP_METHOD = "POST";
    private static final String SERVICE = "aps";

    private static final String SHA_256 = "SHA-256";
    private static final String HMAC_SHA_256 = "HmacSHA256";

    // Format strings for the date/time and date stamps required during signing
    private static final String ISO_8601_BASIC_FORMAT = "yyyyMMdd'T'HHmmss'Z'";
    private static final String DATE_STRING_FORMAT = "yyyyMMdd";

    // Header names
    private static final String X_AMZ_DATE = "x-amz-date";
    private static final String X_AMZ_SECURITY_TOKEN = "x-amz-security-token";
    private static final String HOST = "Host";

    private final URL endpointUrl;
    private final String regionName;

    private final SimpleDateFormat dateTimeFormat;
    private final SimpleDateFormat dateStampFormat;

    /**
     * Create a new Http/1.1 AWS V4a request signer.
     *
     * @param endpointUrl The service endpoint, including the path to any resource.
     * @param regionName The system name of the AWS region associated with the endpoint, e.g.
     *     us-east-1.
     */
    public AWS4SignerForAMP(URL endpointUrl, String regionName) {
        this.endpointUrl = endpointUrl;
        this.regionName = regionName;

        dateTimeFormat = new SimpleDateFormat(ISO_8601_BASIC_FORMAT);
        dateTimeFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
        dateStampFormat = new SimpleDateFormat(DATE_STRING_FORMAT);
        dateStampFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
    }

    private static String urlEncode(String url, boolean keepPathSlash) {

        String encoded;
        try {
            encoded = URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding is not supported.", e);
        }
        if (keepPathSlash) {
            encoded = encoded.replace("%2F", "/");
        }
        return encoded;
    }

    /**
     * Returns the canonical collection of header names that will be included in the signature. For
     * AWS4, all header names must be included in the process in sorted canonicalized order.
     */
    private static String getCanonicalizeHeaderNames(Map<String, String> headers) {
        List<String> sortedHeaders = new ArrayList<String>(headers.keySet());
        sortedHeaders.sort(String.CASE_INSENSITIVE_ORDER);

        StringBuilder buffer = new StringBuilder();
        for (String header : sortedHeaders) {
            if (buffer.length() > 0) {
                buffer.append(";");
            }
            buffer.append(header.toLowerCase());
        }

        return buffer.toString();
    }

    /**
     * Computes the canonical headers with values for the request. For AWS4, all headers must be
     * included in the signing process.
     */
    private static String getCanonicalizedHeaderString(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return "";
        }

        // step1: sort the headers by case-insensitive order
        List<String> sortedHeaders = new ArrayList<>(headers.keySet());
        sortedHeaders.sort(String.CASE_INSENSITIVE_ORDER);

        // step2: form the canonical header:value entries in sorted order.
        // Multiple white spaces in the values should be compressed to a single
        // space.
        StringBuilder buffer = new StringBuilder();
        for (String key : sortedHeaders) {
            buffer.append(key.toLowerCase().replaceAll("\\s+", " "))
                    .append(":")
                    .append(headers.get(key).replaceAll("\\s+", " "));
            buffer.append("\n");
        }

        return buffer.toString();
    }

    /** Returns the canonicalized resource path for the service endpoint. */
    private static String getCanonicalizedResourcePath(URL endpoint) {
        if (endpoint == null) {
            return "/";
        }
        String path = endpoint.getPath();
        if (path == null || path.isEmpty()) {
            return "/";
        }

        String encodedPath = urlEncode(path, true);
        if (encodedPath.startsWith("/")) {
            return encodedPath;
        } else {
            return "/".concat(encodedPath);
        }
    }

    /**
     * Returns the canonical request string to go into the signer process; this consists of several
     * canonical sub-parts.
     */
    private static String getCanonicalRequest(
            URL endpoint,
            String queryParameters,
            String canonicalizedHeaderNames,
            String canonicalizedHeaders,
            String bodyHash) {

        return HTTP_METHOD
                + '\n'
                + getCanonicalizedResourcePath(endpoint)
                + '\n'
                + queryParameters
                + '\n'
                + canonicalizedHeaders
                + '\n'
                + canonicalizedHeaderNames
                + '\n'
                + bodyHash;
    }

    /**
     * Examines the specified query string parameters and returns a canonicalized form.
     *
     * <p>The canonicalized query string is formed by first sorting all the query string parameters,
     * then URI encoding both the key and value and then joining them, in order, separating key
     * value pairs with an '&'.
     *
     * @param parameters The query string parameters to be canonicalized.
     * @return A canonicalized form for the specified query string parameters.
     */
    private static String getCanonicalizedQueryString(Map<String, String> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return "";
        }

        SortedMap<String, String> sorted = new TreeMap<>();

        Iterator<Map.Entry<String, String>> pairs = parameters.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            String key = pair.getKey();
            String value = pair.getValue();
            sorted.put(urlEncode(key, false), urlEncode(value, false));
        }

        StringBuilder builder = new StringBuilder();
        pairs = sorted.entrySet().iterator();
        while (pairs.hasNext()) {
            Map.Entry<String, String> pair = pairs.next();
            builder.append(pair.getKey());
            builder.append("=");
            builder.append(pair.getValue());
            if (pairs.hasNext()) {
                builder.append("&");
            }
        }

        return builder.toString();
    }

    /** Generates the StringToSing. */
    private static String stringToSign(String dateTime, String scope, String canonicalRequest) {
        return SCHEME
                + "-"
                + ALGORITHM
                + "\n"
                + dateTime
                + "\n"
                + scope
                + "\n"
                + toHex(hash(canonicalRequest));
    }

    private static byte[] hmacSHA256Sign(String stringData, byte[] key) {
        try {
            byte[] data = stringData.getBytes(StandardCharsets.UTF_8);
            Mac mac = Mac.getInstance(HMAC_SHA_256);
            mac.init(new SecretKeySpec(key, HMAC_SHA_256));
            return mac.doFinal(data);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to calculate a request signature: " + e.getMessage(), e);
        }
    }

    /** Hashes the string contents (assumed to be UTF-8) using the SHA-256 algorithm. */
    public static byte[] hash(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance(SHA_256);
            md.update(text.getBytes(StandardCharsets.UTF_8));
            return md.digest();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to compute hash while signing request: " + e.getMessage(), e);
        }
    }

    /** Hashes the byte array using the SHA-256 algorithm. */
    public static byte[] hash(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance(SHA_256);
            md.update(data);
            return md.digest();
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to compute hash while signing request: " + e.getMessage(), e);
        }
    }

    /** Converts a byte[] to a lowercase hex string. */
    public static String toHex(byte[] data) {
        return StringUtils.byteToHexString(data);
    }

    /**
     * Computes an AWS4 signature for a request, ready for inclusion as an 'Authorization' header.
     *
     * @param headers The request headers; 'Host' and 'X-Amz-Date' will be added to this set.
     * @param queryParameters Any query parameters that will be added to the endpoint. The
     *     parameters should be specified in canonical format.
     * @param bodyHash Precomputed SHA256 hash of the request body content; this value should also
     *     be set as the header 'X-Amz-Content-SHA256' for non-streaming uploads.
     * @param awsAccessKey The user's AWS Access Key.
     * @param awsSecretKey The user's AWS Secret Key.
     * @return The computed authorization string for the request. This value needs to be set as the
     *     header 'Authorization' on the subsequent HTTP request.
     */
    public String computeSignature(
            Map<String, String> headers,
            Map<String, String> queryParameters,
            String bodyHash,
            String awsAccessKey,
            String awsSecretKey,
            String sessionToken) {
        // first get the date and time for the subsequent request, and convert
        // to ISO 8601 format for use in signature generation
        Date now = new Date();
        String dateTimeStamp = dateTimeFormat.format(now);

        // update the headers with required 'x-amz-date' and 'host' values
        headers.put(X_AMZ_DATE, dateTimeStamp);
        if (sessionToken != null && !sessionToken.isEmpty()) {
            headers.put(X_AMZ_SECURITY_TOKEN, sessionToken);
        }

        String hostHeader = endpointUrl.getHost();
        int port = endpointUrl.getPort();
        if (port > -1) {
            hostHeader = hostHeader.concat(":" + port);
        }
        headers.put(HOST, hostHeader);

        // canonicalize the headers; we need the set of header names as well as the
        // names and values to go into the signature process
        String canonicalizedHeaderNames = getCanonicalizeHeaderNames(headers);
        String canonicalizedHeaders = getCanonicalizedHeaderString(headers);

        // if any query string parameters have been supplied, canonicalize them
        String canonicalizedQueryParameters = getCanonicalizedQueryString(queryParameters);

        // canonicalize the various components of the request
        String canonicalRequest =
                getCanonicalRequest(
                        endpointUrl,
                        canonicalizedQueryParameters,
                        canonicalizedHeaderNames,
                        canonicalizedHeaders,
                        bodyHash);

        // construct the string to be signed
        String dateStamp = dateStampFormat.format(now);
        String scope = dateStamp + "/" + regionName + "/" + SERVICE + "/" + TERMINATOR;
        String stringToSign = stringToSign(dateTimeStamp, scope, canonicalRequest);

        // compute the signing key
        byte[] kSecret = (SCHEME + awsSecretKey).getBytes();
        byte[] kDate = hmacSHA256Sign(dateStamp, kSecret);
        byte[] kRegion = hmacSHA256Sign(regionName, kDate);
        byte[] kService = hmacSHA256Sign(SERVICE, kRegion);
        byte[] kSigning = hmacSHA256Sign(TERMINATOR, kService);
        byte[] signature = hmacSHA256Sign(stringToSign, kSigning);

        String credentialsAuthorizationHeader = "Credential=" + awsAccessKey + "/" + scope;
        String signedHeadersAuthorizationHeader = "SignedHeaders=" + canonicalizedHeaderNames;
        String signatureAuthorizationHeader = "Signature=" + toHex(signature);

        return SCHEME
                + "-"
                + ALGORITHM
                + " "
                + credentialsAuthorizationHeader
                + ", "
                + signedHeadersAuthorizationHeader
                + ", "
                + signatureAuthorizationHeader;
    }
}
