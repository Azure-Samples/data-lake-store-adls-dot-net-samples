---
page_type: sample
languages:
- csharp
products:
- azure
description: "This sample demonstrates how to interact with the Azure Data Lake Storage Gen1 service using the .NET SDK."
urlFragment: data-lake-store-adls-dot-net-samples
---

# Azure Data Lake Storage Gen1 .NET: Additional Samples 

This sample demonstrates how to interact with the Azure Data Lake Storage Gen1 service using the .NET SDK. The sample walks through the following:
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
