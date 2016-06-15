package cloudproject.test;


/*******************************************************************************
 * Copyright (c) 2008, 2010 Xuggle Inc.  All rights reserved.
 *  
 * This file is part of Xuggle-Xuggler-Main.
 *
 * Xuggle-Xuggler-Main is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Xuggle-Xuggler-Main is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Xuggle-Xuggler-Main.  If not, see <http://www.gnu.org/licenses/>.
 *******************************************************************************/

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Opens up a media container, and prints out a summary of the contents.
 * 
 * If you pass -Dxuggle.options we'll also tell you what every
 * configurable option on the container and streams is set to.
 * 
 * @author aclarke
 *
 */
public class getContainerInfor_xuggler{
	
	public getContainerInfor_xuggler(){
		
	}
	
/**
   * A container we'll use to read data from.
   */
  private IContainer mIContainer = null;
  /**
   * A container we'll use to write data from.
   */
  private IContainer mOContainer = null;

  /**
   * A set of {@link IStream} values for each stream in the input
   * {@link IContainer}.
   */
  private IStream[] mIStreams = null;
  /**
   * A set of {@link IStreamCoder} objects we'll use to decode audio and video.
   */
  private IStreamCoder[] mICoders = null;

  /**
   * A set of {@link IStream} objects for each stream we'll output to the output
   * {@link IContainer}.
   */
  private IStream[] mOStreams = null;
  /**
   * A set of {@link IStreamCoder} objects we'll use to encode audio and video.
   */
  private IStreamCoder[] mOCoders = null;

  /**
   * A set of {@link IVideoPicture} objects that we'll use to hold decoded video
   * data.
   */
  private IVideoPicture[] mIVideoPictures = null;
  /**
   * A set of {@link IVideoPicture} objects we'll use to hold
   * potentially-resampled video data before we encode it.
   */
  private IVideoPicture[] mOVideoPictures = null;

  /**
   * A set of {@link IAudioSamples} objects we'll use to hold decoded audio
   * data.
   */
  private IAudioSamples[] mISamples = null;
  /**
   * A set of {@link IAudioSamples} objects we'll use to hold
   * potentially-resampled audio data before we encode it.
   */
  private IAudioSamples[] mOSamples = null;

  /**
   * A set of {@link IAudioResampler} objects (one for each stream) we'll use to
   * resample audio if needed.
   */
  private IAudioResampler[] mASamplers = null;
  /**
   * A set of {@link IVideoResampler} objects (one for each stream) we'll use to
   * resample video if needed.
   */
  private IVideoResampler[] mVSamplers = null;

  /**
   * Should we convert audio
   */
  private boolean mHasAudio = true;
  /**
   * Should we convert video
   */
  private boolean mHasVideo = true;

  /**
   * Should we force an interleaving of the output
   */
  private final boolean mForceInterleave = true;

  /**
   * Should we attempt to encode 'in real time'
   */
  private boolean mRealTimeEncoder;

  private Long mStartClockTime;
  private Long mStartStreamTime;
  
  private IContainerFormat iFmt = null;
  private IContainerFormat oFmt = null;
  
  int retval = 0;
  int numStreams = 0;
  
  
  public int setupContainers(String inputURL, String outputURL){

	
	    /**
	     * Create one container for input, and one for output.
	     */
	    mIContainer = IContainer.make();
	    mOContainer = IContainer.make();
	    
	
	    /**
	     * Open the input container for Reading.
	     */
	    
	
	    retval = mIContainer.open(inputURL, IContainer.Type.READ, iFmt);
	    if (retval < 0)
	      throw new RuntimeException("could not open url: " + inputURL);
	    System.out.println("\n********Open input container**********\n");
	
	       
	    /**
	     * Open the output container for writing. If oFmt is null, we are telling
	     * Xuggler to guess the output container format based on the outputURL.
	     */
	    retval = mOContainer.open(outputURL, IContainer.Type.WRITE, oFmt);
	    if (retval < 0)
	      throw new RuntimeException("could not open output url: " + outputURL);
	    System.out.println("********Open output container**********\n");
	
	
	    /**
	     * Find out how many streams are there in the input container? For example,
	     * most FLV files will have 2 -- 1 audio stream and 1 video stream.
	     */
	    numStreams = mIContainer.getNumStreams();
	    if (numStreams <= 0)
	      throw new RuntimeException("not streams in input url: " + inputURL);
	
	    System.out.printf("file \"%s\": %d stream%s; ",
	  	      inputURL,
	  	      numStreams,
	  	      numStreams == 1 ? "" : "s");
	    
	    final String formattedDuration = formatTimeStamp(mIContainer.getDuration());
		  System.out.printf("\n\nDuration: %s, start: %f, bitrate: %d kb/s\n",
		      formattedDuration,
		      mIContainer.getStartTime() == Global.NO_PTS ? 0 : mIContainer.getStartTime() / 1000000.0,
		          mIContainer.getBitRate()/1000);  
		return numStreams;
		  
		  
  }
  
  public void setupStreams(String inputURL, String outputURL){
	  
	    int astream = -1;
	    int aquality = 0;
	
	    int sampleRate = 0;
	    int channels = 0;
	    int abitrate = 0;
	    int vbitrate = 0;
	    int vbitratetolerance = 0;
	    int vquality = -1;
	    int vstream = -1;
	    double vscaleFactor = 1.0;
	  /**
	     * Here we create IStream, IStreamCoders and other objects for each input
	     * stream.
	     * 
	     * We make parallel objects for each output stream as well.
	     */
	    mIStreams = new IStream[numStreams];
	    mICoders = new IStreamCoder[numStreams];
	    mOStreams = new IStream[numStreams];
	    mOCoders = new IStreamCoder[numStreams];
	    mASamplers = new IAudioResampler[numStreams];
	    mVSamplers = new IVideoResampler[numStreams];
	    mIVideoPictures = new IVideoPicture[numStreams];
	    mOVideoPictures = new IVideoPicture[numStreams];
	    mISamples = new IAudioSamples[numStreams];
	    mOSamples = new IAudioSamples[numStreams];

	    /**
	     * Now let's go through the input streams one by one and explicitly set up
	     * our contexts.
	     */
	    for (int i = 0; i < numStreams; i++)
	    {
	      /**
	       * Get the IStream for this input stream.
	       */
	      IStream is = mIContainer.getStream(i);
	      /**
	       * And get the input stream coder. Xuggler will set up all sorts of
	       * defaults on this StreamCoder for you (such as the audio sample rate)
	       * when you open it.
	       * 
	       * You can create IStreamCoders yourself using
	       * IStreamCoder#make(IStreamCoder.Direction), but then you have to set all
	       * parameters yourself.
	       */
	      IStreamCoder ic = is.getStreamCoder();

	      /**
	       * Find out what Codec Xuggler guessed the input stream was encoded with.
	       */
	      ICodec.Type cType = ic.getCodecType();

	      mIStreams[i] = is;
	      mICoders[i] = ic;
	      mOStreams[i] = null;
	      mOCoders[i] = null;
	      mASamplers[i] = null;
	      mVSamplers[i] = null;
	      mIVideoPictures[i] = null;
	      mOVideoPictures[i] = null;
	      mISamples[i] = null;
	      mOSamples[i] = null;
	      
	      System.out.printf("\n  streamIn %d: ",    i);
		  System.out.printf("type: %s; ",     mICoders[i].getCodecType());
		  System.out.printf("\n            codec: %s; ",    mICoders[i].getCodecID());
		  System.out.printf("\n            GOP number is: %d", mICoders[i].getNumPicturesInGroupOfPictures());
		 //   System.out.printf("\n            duration: %s; ", streamIn[i].getDuration() == Global.NO_PTS ? "unknown" : "" + streamIn[i].getDuration());
		
		  final String formattedStreamDuration = formatStreamTimeStamp(mIStreams[i], mIStreams[i].getDuration());    System.out.printf("\n            duration: %s; ", mIStreams[i].getDuration() == Global.NO_PTS ? "unknown" : "" + formattedStreamDuration);
		  System.out.printf("\n            start time: %s; ", mIContainer.getStartTime() == Global.NO_PTS ? "unknown" : "" + mIStreams[i].getStartTime());
		  System.out.printf("\n            language: %s; ", mIStreams[i].getLanguage() == null ? "unknown" : mIStreams[i].getLanguage());
		  System.out.printf("\n            timebase: %d/%d; ", mIStreams[i].getTimeBase().getNumerator(), mIStreams[i].getTimeBase().getDenominator());
		  System.out.printf("\n            coder tb: %d/%d; ", mICoders[i].getTimeBase().getNumerator(), mICoders[i].getTimeBase().getDenominator());

	      if (cType == ICodec.Type.CODEC_TYPE_AUDIO && mHasAudio
	          && (astream == -1 || astream == i))
	      {
	    	  
	    	  System.out.printf("\n            sample rate: %d; ", mICoders[i].getSampleRate());
	          System.out.printf("\n            channels: %d; ",    mICoders[i].getChannels());
	          System.out.printf("\n            format: %s",        mICoders[i].getSampleFormat());
	          System.out.printf("\n");	  
	          
	          
	        /**
	         * So it looks like this stream as an audio stream. Now we add an audio
	         * stream to the output container that we will use to encode our
	         * resampled audio.
	         */
	        IStream os = mOContainer.addNewStream(i);
	        

	        /**
	         * And we ask the IStream for an appropriately configured IStreamCoder
	         * for output.
	         * 
	         * Unfortunately you still need to specify a lot of things for
	         * outputting (because we can't really guess what you want to encode
	         * as).
	         */
	        IStreamCoder oc = os.getStreamCoder();

	        mOStreams[i] = os;
	        mOCoders[i] = oc;

	       /**
	       * Looks like the user didn't specify an output coder for audio.
	       * 
	       * So we ask Xuggler to guess an appropriate output coded based on the
	       * URL, container format, and that it's audio.
	       */
	        ICodec codec = ICodec.guessEncodingCodec(oFmt, null, outputURL, null,
	          cType);
	        if (codec == null)
	           throw new RuntimeException("could not guess " + cType
	            + " encoder for: " + outputURL);
	        /**
	         * Now let's use that.
	         */
	        oc.setCodec(codec);
	        

	        /**
	         * In general a IStreamCoder encoding audio needs to know: 1) A ICodec
	         * to use. 2) The sample rate and number of channels of the audio. Most
	         * everything else can be defaulted.
	         */

	        /**
	         * If the user didn't specify a sample rate to encode as, then just use
	         * the same sample rate as the input.
	         */
	        if (sampleRate == 0)
	          sampleRate = ic.getSampleRate();
	        oc.setSampleRate(sampleRate);
	        /**
	         * If the user didn't specify a bit rate to encode as, then just use the
	         * same bit as the input.
	         */
	        if (abitrate == 0)
	          abitrate = ic.getBitRate();
	        if (abitrate == 0)
	          // some containers don't give a bit-rate
	          abitrate = 64000;
	        oc.setBitRate(abitrate);
	        
	        /**
	         * If the user didn't specify the number of channels to encode audio as,
	         * just assume we're keeping the same number of channels.
	         */
	        if (channels == 0)
	          channels = ic.getChannels();
	        oc.setChannels(channels);

	        /**
	         * And set the quality (which defaults to 0, or highest, if the user
	         * doesn't tell us one).
	         */
	        oc.setGlobalQuality(aquality);

	        /**
	         * Now check if our output channels or sample rate differ from our input
	         * channels or sample rate.
	         * 
	         * If they do, we're going to need to resample the input audio to be in
	         * the right format to output.
	         */
	        if (oc.getChannels() != ic.getChannels()
	            || oc.getSampleRate() != ic.getSampleRate())
	        {
	          /**
	           * Create an audio resampler to do that job.
	           */
	          mASamplers[i] = IAudioResampler.make(oc.getChannels(), ic
	              .getChannels(), oc.getSampleRate(), ic.getSampleRate());
	          if (mASamplers[i] == null)
	          {
	            throw new RuntimeException(
	                "could not open audio resampler for stream: " + i);
	          }
	        }
	        else
	        {
	          mASamplers[i] = null;
	        }
	        /**
	         * Finally, create some buffers for the input and output audio
	         * themselves.
	         * 
	         * We'll use these repeated during the #run(CommandLine) method.
	         */
	        mISamples[i] = IAudioSamples.make(1024, ic.getChannels());
	        mOSamples[i] = IAudioSamples.make(1024, oc.getChannels());
	        
	        System.out.println("\n********Finish set up output audio coder**********\n");


	      }
	      else if (cType == ICodec.Type.CODEC_TYPE_VIDEO && mHasVideo
	          && (vstream == -1 || vstream == i))
	      {

	    	  System.out.printf("\n            width: %d; ",  mICoders[i].getWidth());
		      System.out.printf("\n            height: %d; ", mICoders[i].getHeight());
		      System.out.printf("\n            format: %s; ", mICoders[i].getPixelType());
		      System.out.printf("\n            frame-rate: %5.2f; ", mICoders[i].getFrameRate().getDouble());
		      System.out.println("\n            frame number: " + mIStreams[i].getNumFrames());
		      System.out.printf("\n");
		      
		      
	        /**
	         * If you're reading these commends, this does the same thing as the
	         * above branch, only for video. I'm going to assume you read those
	         * comments and will only document something substantially different
	         * here.
	         */
	        IStream os = mOContainer.addNewStream(i);
	        IStreamCoder oc = os.getStreamCoder();
	       

	        mOStreams[i] = os;
	        mOCoders[i] = oc;

	        
	        ICodec codec = ICodec.guessEncodingCodec(oFmt, null, outputURL, null,
	          cType);
	        if (codec == null)
	          throw new RuntimeException("could not guess " + cType
	            + " encoder for: " + outputURL);

	        oc.setCodec(codec);
	        

	        /**
	         * In general a IStreamCoder encoding video needs to know: 1) A ICodec
	         * to use. 2) The Width and Height of the Video 3) The pixel format
	         * (e.g. IPixelFormat.Type#YUV420P) of the video data. Most everything
	         * else can be defaulted.
	         */
	        if (vbitrate == 0)
	          vbitrate = ic.getBitRate();
	        if (vbitrate == 0)
	          vbitrate = 250000;
	        oc.setBitRate(vbitrate);
	        if (vbitratetolerance > 0)
	          oc.setBitRateTolerance(vbitratetolerance);

	        int oWidth = ic.getWidth();
	        int oHeight = ic.getHeight();

	        if (oHeight <= 0 || oWidth <= 0)
	          throw new RuntimeException("could not find width or height in url: "
	              + inputURL);

	        /**
	         * For this program we don't allow the user to specify the pixel format
	         * type; we force the output to be the same as the input.
	         */
	        oc.setPixelType(ic.getPixelType());

	        if (vscaleFactor != 1.0)
	        {
	          /**
	           * In this case, it looks like the output video requires rescaling, so
	           * we create a IVideoResampler to do that dirty work.
	           */
	          oWidth = (int) (oWidth * vscaleFactor);
	          oHeight = (int) (oHeight * vscaleFactor);

	          mVSamplers[i] = IVideoResampler
	              .make(oWidth, oHeight, oc.getPixelType(), ic.getWidth(), ic
	                  .getHeight(), ic.getPixelType());
	          if (mVSamplers[i] == null)
	          {
	            throw new RuntimeException(
	                "This version of Xuggler does not support video resampling "
	                    + i);
	          }
	        }
	        else
	        {
	          mVSamplers[i] = null;
	        }
	        oc.setHeight(oHeight);
	        oc.setWidth(oWidth);

	        if (vquality >= 0)
	        {
	          oc.setFlag(IStreamCoder.Flags.FLAG_QSCALE, true);
	          oc.setGlobalQuality(vquality);
	        }

	        /**
	         * TimeBases are important, especially for Video. In general Audio
	         * encoders will assume that any new audio happens IMMEDIATELY after any
	         * prior audio finishes playing. But for video, we need to make sure
	         * it's being output at the right rate.
	         * 
	         * In this case we make sure we set the same time base as the input, and
	         * then we don't change the time stamps of any IVideoPictures.
	         * 
	         * But take my word that time stamps are tricky, and this only touches
	         * the envelope. The good news is, it's easier in Xuggler than some
	         * other systems.
	         */
	        IRational num = null;
	        num = ic.getFrameRate();
	        oc.setFrameRate(num);
	        oc.setTimeBase(IRational.make(num.getDenominator(), num
	                .getNumerator()));
	        num = null;

	        /**
	         * And allocate buffers for us to store decoded and resample video
	         * pictures.
	         */
	        mIVideoPictures[i] = IVideoPicture.make(ic.getPixelType(), ic
	            .getWidth(), ic.getHeight());
	        mOVideoPictures[i] = IVideoPicture.make(oc.getPixelType(), oc
	            .getWidth(), oc.getHeight());
	        
	        
	        System.out.println("\n********Finish set up output video coder**********\n");

	      }
	      else
	      {
	        System.out.println("Ignoring input stream {" + i + "} of type {" + cType + "}");
	      }

	      /**
	       * Now, once you've set up all the parameters on the StreamCoder, you must
	       * open() them so they can do work.
	       * 
	       * They will return an error if not configured correctly, so we check for
	       * that here.
	       */
	      if (mOCoders[i] != null)
	      {
	        retval = mOCoders[i].open();
	        if (retval < 0)
	          throw new RuntimeException(
	              "could not open output encoder for stream: " + i);
	        if(i == 0)
	        System.out.println("\n********open output video coder**********");
	        else
	        System.out.println("\n********open output audio coder**********");



	        
	        retval = mICoders[i].open();
	        if (retval < 0)
	          throw new RuntimeException(
	              "could not open input decoder for stream: " + i);
	        
	        if(i == 0)
	        System.out.println("********open input video coder**********\n");
	        else
	        System.out.println("********open input audio coder**********\n");

	      }
	    }

	    /**
	     * Pretty much every output container format has a header they need written,
	     * so we do that here.
	     * 
	     * You must configure your output IStreams correctly before writing a
	     * header, and few formats deal nicely with key parameters changing (e.g.
	     * video width) after a header is written.
	     */
	    retval = mOContainer.writeHeader();
	    if (retval < 0)
	      throw new RuntimeException("Could not write header for: " + outputURL);
	    System.out.println("\n********Write header to output container**********\n");


	    /**
	     * That's it with setup; we're good to begin!
	     * 
	     */
	    System.out.println("\n********This is all set up, we are ready to begin**********\n");
  }
  
  public void transcode(){

	    
	    IPacket iPacket = IPacket.make();
		IPacket oPacket = IPacket.make();
		
		/**
	     * Keep some "pointers' we'll use for the audio we're working with.
	     */
	    IAudioSamples inSamples = null;
	    IAudioSamples outSamples = null;
	    IAudioSamples reSamples = null;


	    /**
	     * And keep some convenience pointers for the specific stream we're working
	     * on for a packet.
	     */
	    IStreamCoder ic = null;
	    IStreamCoder oc = null;
	    IAudioResampler as = null;
	    IVideoResampler vs = null;
	    IVideoPicture inFrame = null;
	    IVideoPicture reFrame = null;

	    /**
	     * Now, we've already opened the files in #setupStreams(CommandLine). We
	     * just keep reading packets from it until the IContainer returns <0
	     */
	    while (mIContainer.readNextPacket(iPacket) == 0)
	    {
	      /**
	       * Find out which stream this packet belongs to.
	       */
	      int i = iPacket.getStreamIndex();
	      int offset = 0;

	      /**
	       * Find out if this stream has a starting timestamp
	       */
	      IStream stream = mIContainer.getStream(i);
	      long tsOffset = 0;
	      if (stream.getStartTime() != Global.NO_PTS && stream.getStartTime() > 0
	          && stream.getTimeBase() != null)
	      {
	        IRational defTimeBase = IRational.make(1,
	            (int) Global.DEFAULT_PTS_PER_SECOND);
	        tsOffset = defTimeBase.rescale(stream.getStartTime(), stream
	            .getTimeBase());
	      }
	      /**
	       * And look up the appropriate objects that are working on that stream.
	       */
	      ic = mICoders[i];
	      oc = mOCoders[i];
	      as = mASamplers[i];
	      vs = mVSamplers[i];
	      inFrame = mIVideoPictures[i];
	      reFrame = mOVideoPictures[i];
	      inSamples = mISamples[i];
	      reSamples = mOSamples[i];

	      if (oc == null)
	        // we didn't set up this coder; ignore the packet
	        continue;

	      /**
	       * Find out if the stream is audio or video.
	       */
	      ICodec.Type cType = ic.getCodecType();

	     /* if (cType == ICodec.Type.CODEC_TYPE_AUDIO && mHasAudio)
	      {
	        *//**
	         * Decoding audio works by taking the data in the packet, and eating
	         * chunks from it to create decoded raw data.
	         * 
	         * However, there may be more data in a packet than is needed to get one
	         * set of samples (or less), so you need to iterate through the byts to
	         * get that data.
	         * 
	         * The following loop is the standard way of doing that.
	         *//*
	        while (offset < iPacket.getSize())
	        {
	          retval = ic.decodeAudio(inSamples, iPacket, offset);
	          if (retval <= 0)
	            throw new RuntimeException("could not decode audio.  stream: " + i);
	          System.out.println("***Decode audio packet***");

	          
	          if (inSamples.getTimeStamp() != Global.NO_PTS)
	            inSamples.setTimeStamp(inSamples.getTimeStamp() - tsOffset);

	          System.out.println("packet: " + iPacket + " samples: " + inSamples + " offset: " + tsOffset);
	          System.out.println("\n");

	          *//**
	           * If not an error, the decodeAudio returns the number of bytes it
	           * consumed. We use that so the next time around the loop we get new
	           * data.
	           *//*
	          offset += retval;
	          int numSamplesConsumed = 0;
	          *//**
	           * If as is not null then we know a resample was needed, so we do that
	           * resample now.
	           *//*
	          if (as != null && inSamples.getNumSamples() > 0)
	          {
	            retval = as.resample(reSamples, inSamples, inSamples
	                .getNumSamples());

	            outSamples = reSamples;
	          }
	          else
	          {
	            outSamples = inSamples;
	          }

	          *//**
	           * Include call a hook to derivied classes to allow them to alter the
	           * audio frame.
	           *//*

	        //  outSamples = alterAudioFrame(outSamples);

	          *//**
	           * Now that we've resampled, it's time to encode the audio.
	           * 
	           * This workflow is similar to decoding; you may have more, less or
	           * just enough audio samples available to encode a packet. But you
	           * must iterate through.
	           * 
	           * Unfortunately (don't ask why) there is a slight difference between
	           * encodeAudio and decodeAudio; encodeAudio returns the number of
	           * samples consumed, NOT the number of bytes. This can be confusing,
	           * and we encourage you to read the IAudioSamples documentation to
	           * find out what the difference is.
	           * 
	           * But in any case, the following loop encodes the samples we have
	           * into packets.
	           *//*
	          while (numSamplesConsumed < outSamples.getNumSamples())
	          {
	            retval = oc.encodeAudio(oPacket, outSamples, numSamplesConsumed);
	            if (retval <= 0)
	              throw new RuntimeException("Could not encode any audio: "
	                  + retval);
	            System.out.println("***Encode audio packet***");
	            *//**
	             * Increment the number of samples consumed, so that the next time
	             * through this loop we encode new audio
	             *//*
	            numSamplesConsumed += retval;
	            System.out.println("packet: " + oPacket + " samples: " + outSamples + " offset: " + tsOffset);


	            retval = mOContainer.writePacket(oPacket);
	  			 if (retval < 0)
	  		        throw new RuntimeException("could not save packet to container");    
	             System.out.println("***Write packet***");      
	             System.out.println("\n");

	 
	          }
	        }

	      }
	      else */if (cType == ICodec.Type.CODEC_TYPE_VIDEO && mHasVideo)
	      {
	        /**
	         * This encoding workflow is pretty much the same as the for the audio
	         * above.
	         * 
	         * The only major delta is that encodeVideo() will always consume one
	         * frame (whereas encodeAudio() might only consume some samples in an
	         * IAudioSamples buffer); it might not be able to output a packet yet,
	         * but you can assume that you it consumes the entire frame.
	         */
	        IVideoPicture outFrame = null;
	        while (offset < iPacket.getSize())
	        {
	          retval = ic.decodeVideo(inFrame, iPacket, offset);
	          if (retval <= 0)
	            throw new RuntimeException("could not decode any video.  stream: "
	                + i);
	          System.out.println("***Decode video packet***");

	         /* System.out.println("decoded vid ts: " +  inFrame.getTimeStamp() + " pkts ts: " +
	              iPacket.getTimeStamp());*/
	          System.out.println("packet: " + iPacket);

	          System.out.println("\n");
	          
	          if (inFrame.getTimeStamp() != Global.NO_PTS)
	            inFrame.setTimeStamp(inFrame.getTimeStamp() - tsOffset);

	          offset += retval;
	          if (inFrame.isComplete())
	          {
	            if (vs != null)
	            {
	              retval = vs.resample(reFrame, inFrame);
	              if (retval < 0)
	                throw new RuntimeException("could not resample video");
	              outFrame = reFrame;
	            }
	            else
	            {
	              outFrame = inFrame;
	            }

	            /**
	             * Include call a hook to derivied classes to allow them to alter
	             * the audio frame.
	             */

	         //   outFrame = alterVideoFrame(outFrame);
	           
	            outFrame.setQuality(0);
	            retval = oc.encodeVideo(oPacket, outFrame, -1);
	            if (retval < 0)
	              throw new RuntimeException("could not encode video");
	            /*do{
	               oc.encodeVideo(oPacket, null, 0);
	            }while(!oPacket.isComplete());*/
	            
	            /*oPacket.setDuration(tsInterval);
	            oPacket.setDts(lastVideoPts);
	            oPacket.setPts(lastVideoPts);
	            oPacket.setPosition(lastPos_out);
	            oPacket+=packet_out.getSize();
	            oPacket.setKeyPacket(true);
*/	            
	           
	            
	            System.out.println("***Encode video packet***");
	            System.out.println("packet: " + oPacket);
	            
	            if(oPacket.isComplete()) {
	            	 
		            retval = mOContainer.writePacket(oPacket);
		  			 if (retval < 0)
		  		        throw new RuntimeException("could not save packet to container");
	                System.out.println("***Write packet***");   

	            }
	            System.out.println("\n");

	           }
	        }
	      }
	      else
	      {
	        /**
	         * Just to be complete; there are other types of data that can show up
	         * in streams (e.g. SUB TITLE).
	         * 
	         * Right now we don't support decoding and encoding that data, but youc
	         * could still decide to write out the packets if you wanted.
	         */
	        System.out.println("ignoring packet of type: "+ cType);
	      }

	    }
	  
		/*int videoPacketCount = 0;
	    int packetCount = 0;
	    int videoStreamIndex = 0;
	    int audioStreamIndex = 1;
		  
	    ArrayList <String> frameList = new ArrayList<String>();
		 // ArrayList <Long> gopDuration = new ArrayList<Long>(); 
	    long gopSize = 0; 
	    long previousKeyPacketPosition = 0;
	    long currentKeyPacketPosition = 0;
	    long gopPts = 0;
	    long previousKeyPacketPts = 0;
	    long currentKeyPacketPts = 0;
	    long gopDts = 0;
	    long previousKeyPacketDts = 0;
	    long currentKeyPacketDts = 0;
	    long gopPosition = 0;
	    long gopDuration = 0;
	    
	    while (mIContainer.readNextPacket(iPacket) >= 0) {
	    	
	    	if(iPacket.isComplete()) {
		    	 
	    	    retval = mOContainer.writePacket(iPacket, true);
			    if (retval < 0)
		        throw new RuntimeException("could not save packet to container"); 
	    	
		    gopDuration += iPacket.getDuration();
		    if(iPacket.getStreamIndex() == videoStreamIndex) {     
			     videoPacketCount++;
			     System.out.println("\nvideo packet number: " + videoPacketCount);   
			     System.out.println("  stream index: " + iPacket.getStreamIndex());
			     System.out.println("  key: " + iPacket.isKey());
			     System.out.println("  position: " + iPacket.getPosition());
			    // System.out.println("  duration: " + iPacket.getDuration());
			
			     
			     final String formattedPacketDuration = formatPacketTimeStamp(iPacket, iPacket.getDuration());
			     System.out.println("  duration: " + formattedPacketDuration);
			     System.out.println("  size: " + iPacket.getSize());
			     System.out.println("  dts: " + formatPacketTimeStamp(iPacket, iPacket.getDts()));
			     System.out.println("  pts: " + formatPacketTimeStamp(iPacket, iPacket.getPts()));
			     
			
			   //  System.out.println(iPacket.toString());
			     //System.out.printf("\n");  
			     
			     if(iPacket.isComplete()) {
			    	 
			    	    retval = mOContainer.writePacket(iPacket, true);
	       			    if (retval < 0)
	       		        throw new RuntimeException("could not save packet to container");   
	       			   // System.out.println("Writing this packet to output container");
				      
					     if(iPacket.isKeyPacket()) {
						      System.out.println("This is a key packet..");
			
					    	   
					    	 //Calculate GOP size I, the previous GOP size will current I frame position
			       		  //minus previous I frame position.
			       		  currentKeyPacketPosition = iPacket.getPosition();
			       		  gopSize = currentKeyPacketPosition - previousKeyPacketPosition;
			       		  gopPosition = previousKeyPacketPosition;
			       		  previousKeyPacketPosition = currentKeyPacketPosition;
			       		  
			       		  //Calculate GOP pts (deadline). It should the first frame in this GOP's pts, most time
			       		  //is key frame.
			       		  gopPts = previousKeyPacketPts;
			       		  currentKeyPacketPts = iPacket.getPts();
			       		  previousKeyPacketPts = currentKeyPacketPts;
			       		  
			       		  gopDts = previousKeyPacketDts;
			       		  currentKeyPacketDts = iPacket.getDts();
			       		  previousKeyPacketDts = currentKeyPacketDts; 
					    	   
					          //Create GOP packet
			       		  oPacket.setPosition(gopPosition);
			       		  oPacket.setPts(gopPts);
			       		  oPacket.setDts(gopDts);
			       		  oPacket.setDuration(gopDuration);
			       		  oPacket.setKeyPacket(true);
			       		  oPacket.setStreamIndex(videoStreamIndex);
			       		  oPacket.setComplete(true, (int) gopSize);
			       		  
			       		  if(oPacket.isComplete()){
			       			
			
			       			 retval = mOContainer.writePacket(oPacket);
			       			 if (retval < 0)
			       		        throw new RuntimeException("could not save packet to container");
			  		
			       		  }         
					    }
				    }
			    }

	      }*/
	    System.out.println("\n********Finish spliting stream to GOP packets**********\n");

	    
	  
	   /**
	   * Close and release all resources we used to run this program.
	   */

	    /**
	     * Some video coders (e.g. MP3) will often "read-ahead" in a stream and keep
	     * extra data around to get efficient compression. But they need some way to
	     * know they're never going to get more data. The convention for that case
	     * is to pass null for the IMediaData (e.g. IAudioSamples or IVideoPicture)
	     * in encodeAudio(...) or encodeVideo(...) once before closing the coder.
	     * 
	     * In that case, the IStreamCoder will flush all data.
	     */
	    for (int i = 0; i < numStreams; i++)
	    {
	      if (mOCoders[i] != null)
	      {
	        oPacket = IPacket.make();
	        do {
	          if (mOCoders[i].getCodecType() == ICodec.Type.CODEC_TYPE_AUDIO)
	            mOCoders[i].encodeAudio(oPacket, null, 0);
	          else
	            mOCoders[i].encodeVideo(oPacket, null, 0);
	          if (oPacket.isComplete())
	            mOContainer.writePacket(oPacket, mForceInterleave);
	        } while (oPacket.isComplete());
	      }
	    }
	    
	    System.out.println("\n********Flush all data from IStreamCoder**********\n");
  }
  
  public void closeStreams(){

	    /**
	     * Some container formats require a trailer to be written to avoid a corrupt
	     * files.
	     * 
	     * Others, such as the FLV container muxer, will take a writeTrailer() call
	     * to tell it to seek() back to the start of the output file and write the
	     * (now known) duration into the Meta Data.
	     * 
	     * So trailers are required. In general if a format is a streaming format,
	     * then the writeTrailer() will never seek backwards.
	     * 
	     * Make sure you don't close your codecs before you write your trailer, or
	     * we'll complain loudly and not actually write a trailer.
	     */
	    retval = mOContainer.writeTrailer();
	    if (retval < 0)
	      throw new RuntimeException("Could not write trailer to output file");
	    
	    
	    System.out.println("\n********Write trailer to output container**********\n");

	    /**
	     * We do a nice clean-up here to show you how you should do it.
	     * 
	     * That said, Xuggler goes to great pains to clean up after you if you
	     * forget to release things. But still, you should be a good boy or giral
	     * and clean up yourself.
	     */
	    for (int i = 0; i < numStreams; i++)
	    {
	      if (mOCoders[i] != null)
	      {
	        /**
	         * And close the input coder to tell Xuggler it can release all native
	         * memory.
	         */
	        mOCoders[i].close();
	      }
	      mOCoders[i] = null;
	      if (mICoders[i] != null)
	        /**
	         * Close the input coder to tell Xuggler it can release all native
	         * memory.
	         */
	        mICoders[i].close();
	      mICoders[i] = null;
	    }
	    
	    System.out.println("\n********Close up coders**********\n");

	    
	    /**
	     * Tell Xuggler it can close the output file, write all data, and free all
	     * relevant memory.
	     */
	    mOContainer.close();
	    System.out.println("\n********Close up output container**********\n");

	    /**
	     * And do the same with the input file.
	     */
	    mIContainer.close();
	    System.out.println("\n********Close up input container**********\n");


	    /**
	     * Technically setting everything to null here doesn't do anything but tell
	     * Java it can collect the memory it used.
	     * 
	     * The interesting thing to note here is that if you forget to close() a
	     * Xuggler object, but also loose all references to it from Java, you won't
	     * leak the native memory. Instead, we'll clean up after you, but we'll
	     * complain LOUDLY in your logs, so you really don't want to do that.
	     */
	    mOContainer = null;
	    mIContainer = null;
	    mISamples = null;
	    mOSamples = null;
	    mIVideoPictures = null;
	    mOVideoPictures = null;
	    mOCoders = null;
	    mICoders = null;
	    mASamplers = null;
	    mVSamplers = null;
   }

 
	
	
/**
   * Pretty prints a timestamp (in {@link Global.NO_PTS} units) into a string.
   * @param duration A timestamp in {@link Global.NO_PTS} units).
   * @return A string representing the duration.
   */
  private static String formatTimeStamp(long duration) {
    if (duration == Global.NO_PTS) {
      return "00:00:00.00";
    }
   // System.out.print("Globle: " + Global.DEFAULT_PTS_PER_SECOND);
    double d = 1.0 * duration / Global.DEFAULT_PTS_PER_SECOND;
    int hours = (int) (d / (60*60));
    int mins = (int) ((d - hours*60*60) / 60);
    int secs = (int) (d - hours*60*60 - mins*60);
    int subsecs = (int)((d - (hours*60*60.0 + mins*60.0 + secs))*100.0);
    return String.format("%1$02d:%2$02d:%3$02d.%4$02d", hours, mins, secs, subsecs);
  }	
  
  private static String formatStreamTimeStamp(IStream stream, long duration) {
	    if (duration == Global.NO_PTS) {
	      return "00:00:00.00";
	    }
	   // System.out.print("Globle: " + Global.DEFAULT_PTS_PER_SECOND);
	    double d = 1.0 * duration / stream.getTimeBase().getDenominator();
	    int hours = (int) (d / (60*60));
	    int mins = (int) ((d - hours*60*60) / 60);
	    int secs = (int) (d - hours*60*60 - mins*60);
	    int subsecs = (int)((d - (hours*60*60.0 + mins*60.0 + secs))*100.0);
	    return String.format("%1$02d:%2$02d:%3$02d.%4$02d", hours, mins, secs, subsecs);
  }	  
  
  private static String formatPacketTimeStamp(IPacket packet, long duration) {
	    if (duration == Global.NO_PTS) {
	      return "00:00:00.00";
	    }
	   // System.out.print("Globle: " + Global.DEFAULT_PTS_PER_SECOND);
	    double d = 1.0 * duration / packet.getTimeBase().getDenominator();
	    int hours = (int) (d / (60*60));
	    int mins = (int) ((d - hours*60*60) / 60);
	    int secs = (int) (d - hours*60*60 - mins*60);
	    int subsecs = (int)((d - (hours*60*60.0 + mins*60.0 + secs))*100.0);
	    return String.format("%1$02d:%2$02d:%3$02d.%4$02d", hours, mins, secs, subsecs);
  }	
  

  public static void main(String arg[]){

	  //Print out everything in the console into the file
	  printOutputFile pof = new printOutputFile();  
	  pof.printOutToFile("BigBuckBunny_320x180_v5"); 
		
	  String inputURL = "/Users/lxb200709/Documents/TransCloud/videosource/inputvideo/elephants dream_01.ts";
	  String outputURL = "/Users/lxb200709/Documents/TransCloud/videosource/elephants dream_01.ts";
      
	  getContainerInfor_xuggler ts = new getContainerInfor_xuggler();
	  ts.setupContainers(inputURL, outputURL);
	  ts.setupStreams(inputURL, outputURL);
	  ts.transcode();
	  ts.closeStreams();
	  
      	  
	  
	  
  }
}
