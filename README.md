# Flows Server

The Proliferation Flows Server is a Complex Event Process designed for all purpose use, with several enhancements that are aimed at surpassing the capabilities of other complex event processors.

The current iteration of Flows is not targeted to any particular data source type, business need, industrial use, or product adaptation.  It may connect to other products with such functions as part of regular use.

# And what makes Flows so special (CEP ain't new)

1) ... and CEP ain't cheap.
2) ... and CEP doesn't fit on Cell Phones because its execution requirements are so low
3) ... and CEP doesn't usually include languages as a in-line option
4) ... and many CEP don't like forwarding to outside interfaces

Seriously, though-- Flows was built with the idea that it's possible to optimize tools normally used for SIEM, Logging, social media collecting, web scraping, etc. into a very small package, reducing what is normally 3-4 servers of reference hardware down to a single core of a virtual machine.  Besides optimization, many complex event processors were surveyed for what is common between them, and a determination was made as to what a "complete complex event processor" is.  With business drivers removed from the design equation, issues like charging per transaction or preventing interaction with other toolsets, the design of Flows should be dreamlike to engineers instead of a reason to quit their job (this has happened..)

Flows is also to be paired with use by Absolution, a forensic toolkit that processes files and returns the data and metadata associated with them.  I.e., given a random directory of crap, it determines what the contents of each file and proceeds to extract data and imply metadata from that file.  One could imagine that Flows collects the data in real time and Absolution determines what the data is.  This could, in turn, be used for training Artificial Intelligence tools, provide sentiment information, locate the needle in the haystack, etc.

# Project Status (DON'T USE YET)

As of version 1.0.2, Proliferation is not ready for general consumption but the code base is being uploaded to GitHub / NuGet for the advantages of revision control, provided features granted to the community, documentation and project tracking, and various other perks that should lead to a faster developing and more ambitious results.

The original project was written for .NET Framework and is in the process of being converted to .NET Core.  The main reason the Server won't currently work is because several tools are needed to complete the distribution:  the configuration system needs to be rewritten from it's Registry based installation, the setup tool needs to support the new configuration system, the API needs to be rewritten from .Framework to .Net Core because the HTTP server it uses is incompatible, and finally the Web Forms interface isn't supported by .NET Core either.
