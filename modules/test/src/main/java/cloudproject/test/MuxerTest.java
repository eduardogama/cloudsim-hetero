package cloudproject.test;

import io.humble.video.Decoder;
import io.humble.video.Demuxer;
import io.humble.video.DemuxerStream;
import io.humble.video.MediaPacket;
import io.humble.video.Muxer;



public class MuxerTest {

 

  /**
   * Test remuxing from mp4 to mov
   */
  
  public void main(String arg[]) throws Exception {
   // final URL s = this.getClass().getResource("/Users/lxb200709/Documents/TransCloud/videosource/BigBuckBunny_320x180.mp4");
    final String f = "/Users/lxb200709/Documents/TransCloud/videosource/BigBuckBunny_320x180.mp4";
    final String o = "/Users/lxb200709/Documents/TransCloud/videosource/BigBuckBunny_320x180_v4.mp4";
    
    Demuxer demuxer = Demuxer.make();
    demuxer.open(f, null, false, true, null, null);
    
    // open output
    Muxer muxer = Muxer.make(o, null, null);
    int n = demuxer.getNumStreams();
    for(int i = 0; i < n; i++) {
      DemuxerStream ds = demuxer.getStream(i);
      Decoder d = ds.getDecoder();
      muxer.addNewStream(d);
    }
    muxer.open(null, null);
    MediaPacket packet = MediaPacket.make();
    while(demuxer.read(packet) >= 0) {
      
      muxer.write(packet, false);
    }
    muxer.close();
    
    // now, let's read in the file we wrote and confirm correct # of packets and size.
    Demuxer demuxer2 = Demuxer.make();
    demuxer2.open(o,  null, false, true, null, null);
    int numPackets = 0;
    while(demuxer2.read(packet) >= 0)
      ++numPackets;
    
    System.out.println("The number of packets: " + numPackets);
 /*   assertEquals(demuxer2.getFileSize(), demuxer.getFileSize(), 1024);
    assertEquals(1162, numPackets);
*/
    demuxer.close();
    demuxer2.close();
  }

}
