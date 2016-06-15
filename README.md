# CVS: A Cost-Efficient and QoS-Aware Video Streaming Using Cloud Services #

Video streams, either in form of on-demand stream- ing or live streaming, usually have to be converted (i.e., transcoded) based on the characteristics of clients’ devices (e.g., spatial resolution, network bandwidth, and supported formats). Transcoding is a computationally expensive and time-consuming operation, therefore, streaming service providers currently store numerous transcoded versions of the same video to serve different types of client devices. Due to the expense of maintaining and upgrading storage and computing infrastructures, many streaming service providers (e.g., Netflix) recently are becoming reliant on cloud services. However, the challenge in utilizing cloud services for video transcoding is how to deploy cloud resources in a cost-efficient manner without any major impact on the quality of video streams. To address this challenge, in this paper, we present the Cloud-based Video Streaming (CVS) architecture to transcode video streams in an on-demand manner. The architecture provides a platform for streaming service providers to utilize cloud resources in a cost-efficient manner and with respect to the Quality of Service (QoS) demands of video streams. In particular, the architecture includes a QoS-aware scheduling method to efficiently map video streams to cloud resources, and a cost-aware dynamic (i.e., elastic) resource provisioning policy that adapts the resource acquisition with respect to the video streaming QoS demands. Simulation results based on realistic cloud traces and with various workload conditions, demonstrate that the CVS architecture can satisfy video streaming QoS demands and reduces the incurred cost of stream providers up to 70%.

# Key Contributions #

  * Proposing the Cloud-based Video Streaming (CVS) architecture that enables streaming service providers to utilize cloud services with minimum cost and maximum user satisfaction.
  * Developing a QoS-aware scheduling method to map transcoding tasks on cloud resources with minimum deadline miss rate and minimum start up delay.
  * Proposing a dynamic resource provisioning policy that minimizes the incurred cost to the streaming service providers without any major impact on the video streams’ QoS.
  * Analyzing the behavior of the scheduling methods and dynamic resource provisioning policy from different perspectives and under various workloads.
  * Discussing the trade-off involved in configuring the dynamic resource provisioning policy.
  

# Achitecture #

<img src="https://gitlab.com/lxb200709/cloudsim/raw/master/images/architecture.png" alt="Drawing" width="500"/>


# Publications #

  * Xiangbo Li, Mohsen Amini Salehi, Magdi Bayoumi, Rajkumar Buyya, “CVS: A Cost-Efficient and QoS-Aware Video Streaming Using Cloud Services”, Submitted to 16th ACM/IEEE International Conference on Cluster Cloud and Grid Computing (CCGrid ’16), Columbia, May 2016.
  * Xiangbo Li, Mohsen Amini Salehi, Magdi Bayoumi, “QoS-Aware Cloud-Based Spatial Resolution Reduction Video Transcoding”, Submitted to 41th IEEE International Conference on Acoustic, Speech, and Signal Processing (ICASSP ’16), China, Mar. 2016.
  * Xiangbo Li, Mohsen Amini Salehi, Magdi Bayoumi, “Cloud-Based Video Streaming for Energy- and Compute-Limited Thin Clients”, Accepted in Stream2015 Workshop at Indiana University, Indianapolis, USA, Oct. 2015.