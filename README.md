---
services: data-lake-store
platforms: .NET
author: rahuldutta90
---

# Azure Data Lake Store .NET: Additional Samples 

This sample demonstrates how to interact with Azure Data Lake Store service using the .NET SDK. The sample walk through following:
- Acquire an Azure ActiveDirectory OAuth token (ServiceClientCredential) using username and password.
- Acquire an Azure ActiveDirectory OAuth token (ServiceClientCredential) using client id and client secret.
- Create client using the account path and Azure ActiveDirectory OAuth token.
- Get a write stream, write a file on store and do flush.
- Get a read stream, perform seek to a particular offset and read from the offset.
- Concatenate two files.
- Use async operations to create a sample hiererchial directory tree on store and then get the content summary.
- Illustrate token refresh.
- Illustrate Bulk upload and download
- Illustrate recursive acl processor
- Illustrate recursive acl and disk usage dump
