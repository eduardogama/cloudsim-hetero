package cloudproject.test;

import io.humble.video.*;
import io.humble.video.AudioFormat.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Transcoder
{
    private String inputUrl = null;
    private String outputUrl = null;
    /**
     * Create a new Converter object.
     */
    public Transcoder(String inputUrl, String outputUrl){
    	this.inputUrl = inputUrl;
    	this.outputUrl = outputUrl;
    	
    }

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * A container we'll use to read data from.
     */
    private Demuxer mIContainer = null;
    /**
     * A container we'll use to write data from.
     */
    private Muxer mOContainer = null;

    /**
     * A set of {@link ContainerStream} values for each stream in the input
     * {@link Container}.
     */
    private ContainerStream[] mContainerStreams = null;
    /**
     * A set of {@link Coder} objects we'll use to decode audio and video.
     */
    private Decoder[] mICoders = null;

    /**
     * A set of {@link ContainerStream} objects for each stream we'll output to the output
     * {@link Container}.
     */
    private ContainerStream[] mOStreams = null;
    /**
     * A set of {@link Coder} objects we'll use to encode audio and video.
     */
    private Encoder[] mOCoders = null;

    /**
     * A set of {@link MediaPicture} objects that we'll use to hold decoded video
     * data.
     */
    private MediaPicture[] mMediaPictures = null;
    /**
     * A set of {@link MediaPicture} objects we'll use to hold
     * potentially-resampled video data before we encode it.
     */
    private MediaPicture[] mOVideoPictures = null;

    /**
     * A set of {@link MediaAudio} objects we'll use to hold decoded audio
     * data.
     */
    private MediaAudio[] mISamples = null;
    /**
     * A set of {@link MediaAudio} objects we'll use to hold
     * potentially-resampled audio data before we encode it.
     */
    private MediaAudio[] mOSamples = null;

    /**
     * A set of {@link MediaAudioResampler} objects (one for each stream) we'll use to
     * resample audio if needed.
     */
    private MediaAudioResampler[] mASamplers = null;
    /**
     * A set of {@link MediaPictureResampler} objects (one for each stream) we'll use to
     * resample video if needed.
     */
    private MediaPictureResampler[] mVSamplers = null;

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

    /**
     * Open an initialize all Xuggler objects needed to encode and decode a video
     * file.
     *
     * @return Number of streams in the input file, or <= 0 on error.
     */

    int setupStreams() throws IOException, InterruptedException
    {
        mHasAudio = true;
        mHasVideo = true;

        mRealTimeEncoder = false;

        String acodec = "libmp3lame";
        String vcodec = "libx264";
        String containerFormat = "mp4";
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

        String icontainerFormat = null;
        String iacodec = null;
        int isampleRate = 0;
        int ichannels = 0;

        //String cpreset = "/usr/share/avconv/libx264-ultrafast.avpreset";
        String cpreset = null;
        String apreset = null;
        String vpreset = null;

        // Should have everything now!

        /**
         * Create one container for input, and one for output.
         */
        mIContainer = Demuxer.make();
        mOContainer = null;

        DemuxerFormat iFmt = null;
        MuxerFormat oFmt = null;

        // override input format
        if (icontainerFormat != null) {
            /**
             * Try to find an output format based on what the user specified, or
             * failing that, based on the outputURL (e.g. if it ends in .flv, we'll
             * guess FLV).
             */
            iFmt = DemuxerFormat.findFormat(icontainerFormat);
            if (iFmt == null) {
                throw new RuntimeException("could not find input container format: " + icontainerFormat);
            }
        }

        // override the input codec
        if (iacodec != null) {
            /**
             * Looks like they did specify one; let's look it up by name.
             */
            Codec codec = Codec.findDecodingCodecByName(iacodec);
            if (codec == null || codec.getType() != MediaDescriptor.Type.MEDIA_AUDIO) {
                throw new RuntimeException("could not find decoder: " + iacodec);
            }
            /**
             * Now, tell the stream coder that it's to use that codec.
             */
            mIContainer.setForcedAudioCodec(codec.getID());
        }

        /**
         * Open the input container for Reading.
         */
        KeyValueBag parameters = KeyValueBag.make();

        if (isampleRate > 0) {
            parameters.setValue("sample_rate", "" + isampleRate);
        }

        if (ichannels > 0) {
            parameters.setValue("channels", "" + ichannels);
        }

        KeyValueBag rejectParameters = KeyValueBag.make();

        mIContainer.open(this.inputUrl, iFmt, false, true, parameters, rejectParameters);
        if (rejectParameters.getNumKeys() > 0) {
            throw new RuntimeException("some parameters were rejected: " + rejectParameters);
        }
        /**
         * If the user EXPLICITLY asked for a output container format, we'll try to
         * honor their request here.
         */
        if (containerFormat != null) {
            /**
             * Try to find an output format based on what the user specified, or
             * failing that, based on the outputURL (e.g. if it ends in .flv, we'll
             * guess FLV).
             */
            oFmt = MuxerFormat.guessFormat(containerFormat, this.outputUrl, null);
            if (oFmt == null) {
                throw new RuntimeException("could not find output container format: " + containerFormat);
            }
        }

        /**
         * Open the output container for writing. If oFmt is null, we are telling
         * Xuggler to guess the output container format based on the outputURL.
         */
        mOContainer = Muxer.make(this.outputUrl, oFmt, null);
        if (mOContainer == null) {
            throw new RuntimeException("could not open output url: " + this.outputUrl);
        }

        if (cpreset != null) {
            Configuration.configure(cpreset, mOContainer);
        }

        /**
         * Find out how many streams are there in the input container? For example,
         * most FLV files will have 2 -- 1 audio stream and 1 video stream.
         */
        int numStreams = mIContainer.getNumStreams();
        if (numStreams <= 0) {
            throw new RuntimeException("not streams in input url: " + this.inputUrl);
        }

        /**
         * Here we create ContainerStream, Coders and other objects for each input
         * stream.
         *
         * We make parallel objects for each output stream as well.
         */
        mContainerStreams = new ContainerStream[numStreams];
        mICoders = new Decoder[numStreams];
        mOStreams = new MuxerStream[numStreams];
        mOCoders = new Encoder[numStreams];
        mASamplers = new MediaAudioResampler[numStreams];
        mVSamplers = new MediaPictureResampler[numStreams];
        mMediaPictures = new MediaPicture[numStreams];
        mOVideoPictures = new MediaPicture[numStreams];
        mISamples = new MediaAudio[numStreams];
        mOSamples = new MediaAudio[numStreams];

        /**
         * Now let's go through the input streams one by one and explicitly set up
         * our contexts.
         */
        for (int i = 0; i < numStreams; i++) {
            /**
             * Get the ContainerStream for this input stream.
             */
            DemuxerStream is = mIContainer.getStream(i);
            /**
             * And get the input stream coder. Xuggler will set up all sorts of
             * defaults on this StreamCoder for you (such as the audio sample rate)
             * when you open it.
             *
             * You can create Coders yourself using
             * Coder#make(Coder.Direction), but then you have to set all
             * parameters yourself.
             */
            Decoder ic = is.getDecoder();

            /**
             * Find out what Codec Xuggler guessed the input stream was encoded with.
             */
            MediaDescriptor.Type cType = ic.getCodecType();

            mContainerStreams[i] = is;
            mICoders[i] = ic;
            mOStreams[i] = null;
            mOCoders[i] = null;
            mASamplers[i] = null;
            mVSamplers[i] = null;
            mMediaPictures[i] = null;
            mOVideoPictures[i] = null;
            mISamples[i] = null;
            mOSamples[i] = null;

            /*if (cType == MediaDescriptor.Type.MEDIA_AUDIO && mHasAudio && (astream == -1 || astream == i)) {
                *//**
                 * First, did the user specify an audio codec?
                 *//*
                Codec codec = null;
                if (acodec != null) {
                    *//**
                     * Looks like they did specify one; let's look it up by name.
                     *//*
                    codec = Codec.findEncodingCodecByName(acodec);
                    if (codec == null || codec.getType() != cType) {
                        throw new RuntimeException("could not find encoder: " + acodec);
                    }

                }
                else {
                    *//**
                     * Looks like the user didn't specify an output coder for audio.
                     *
                     * So we ask Xuggler to guess an appropriate output coded based on the
                     * URL, container format, and that it's audio.
                     *//*
                    codec = Codec.guessEncodingCodec(oFmt, null, this.outputUrl, null, cType);
                    if (codec == null) {
                        throw new RuntimeException("could not guess " + cType + " encoder for: " + this.outputUrl);
                    }
                }
                *//**
                 * So it looks like this stream as an audio stream. Now we add an audio
                 * stream to the output container that we will use to encode our
                 * resampled audio.
                 *//*
                Encoder oc = Encoder.make(codec);

                *//**
                 * Now let's see if the codec can support the input sample format; if not
                 * we pick the last sample format the codec supports.
                 *//*
                AudioFormat.Type preferredFormat = ic.getSampleFormat();

                List<AudioFormat.Type> formats = (List<Type>) codec.getSupportedAudioFormats();
                for (AudioFormat.Type format : formats) {
                    oc.setSampleFormat(format);
                    if (format == preferredFormat) {
                        break;
                    }
                }

                if (apreset != null) {
                    Configuration.configure(apreset, oc);
                }

                *//**
                 * In general a Coder encoding audio needs to know: 1) A Codec
                 * to use. 2) The sample rate and number of channels of the audio. Most
                 * everything else can be defaulted.
                 *//*

                *//**
                 * If the user didn't specify a sample rate to encode as, then just use
                 * the same sample rate as the input.
                 *//*
                if (sampleRate == 0) {
                    sampleRate = ic.getSampleRate();
                }
                oc.setSampleRate(sampleRate);
                *//**
                 * If the user didn't specify a bit rate to encode as, then just use the
                 * same bit as the input.
                 *//*
                if (abitrate == 0) {
                    abitrate = ic.getSampleRate();
                }
                if (abitrate == 0) {
                    // some containers don't give a bit-rate
                    abitrate = 64000;
                }
                oc.setSampleRate(abitrate);

                *//**
                 * If the user didn't specify the number of channels to encode audio as,
                 * just assume we're keeping the same number of channels.
                 *//*
                if (channels == 0) {
                    channels = ic.getChannels();
                }
                oc.setChannels(channels);

                *//**
                 * And set the quality (which defaults to 0, or highest, if the user
                 * doesn't tell us one).
                 *//*
                //TODO BRAM: I didn't find this option in Humble
                //oc.setGlobalQuality(aquality);

                oc.setChannelLayout(ic.getChannelLayout());

                oc.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);

                //TODO BRAM: ok to pass nulls here?
                oc.open(null, null);
                ContainerStream os = mOContainer.addNewStream(oc);

                mOStreams[i] = os;
                mOCoders[i] = oc;

                *//**
                 * Now check if our output channels or sample rate differ from our input
                 * channels or sample rate.
                 *
                 * If they do, we're going to need to resample the input audio to be in
                 * the right format to output.
                 *//*
                if (oc.getChannels() != ic.getChannels() || oc.getSampleRate() != ic.getSampleRate() || oc.getSampleFormat() != ic.getSampleFormat()) {
                    *//**
                     * Create an audio resampler to do that job.
                     *//*
                    mASamplers[i] = MediaAudioResampler.make(oc.getChannelLayout(), oc.getSampleRate(), oc.getSampleFormat(), ic.getChannelLayout(), ic.getSampleRate(), ic.getSampleFormat());
                    if (mASamplers[i] == null) {
                        throw new RuntimeException("could not open audio resampler for stream: " + i);
                    }
                }
                else {
                    mASamplers[i] = null;
                }
                *//**
                 * Finally, create some buffers for the input and output audio
                 * themselves.
                 *
                 * We'll use these repeated during the #run(CommandLine) method.
                 *//*
                mISamples[i] = MediaAudio.make(1024, ic.getSampleRate(), ic.getChannels(), ic.getChannelLayout(), ic.getSampleFormat());
                mOSamples[i] = MediaAudio.make(1024, oc.getSampleRate(), oc.getChannels(), oc.getChannelLayout(), oc.getSampleFormat());
            }*/
            if (cType == MediaDescriptor.Type.MEDIA_VIDEO && mHasVideo && (vstream == -1 || vstream == i)) {
                /**
                 * If you're reading these commends, this does the same thing as the
                 * above branch, only for video. I'm going to assume you read those
                 * comments and will only document something substantially different
                 * here.
                 */
                Codec codec = null;
                if (vcodec != null) {
                    codec = Codec.findEncodingCodecByName(vcodec);
                    if (codec == null || codec.getType() != cType) {
                        throw new RuntimeException("could not find encoder: " + vcodec);
                    }
                }
                else {
                    codec = Codec.guessEncodingCodec(oFmt, null, this.outputUrl, null, cType);
                    if (codec == null) {
                        throw new RuntimeException("could not guess " + cType + " encoder for: " + this.outputUrl);
                    }

                }

                Encoder oc = Encoder.make(codec);

                // Set options AFTER selecting codec
                if (vpreset != null) {
                    Configuration.configure(vpreset, oc);
                }

                /**
                 * In general a Coder encoding video needs to know: 1) A Codec
                 * to use. 2) The Width and Height of the Video 3) The pixel format
                 * (e.g. IPixelFormat.Type#YUV420P) of the video data. Most everything
                 * else can be defaulted.
                 */
                if (vbitrate == 0) {
                    //TODO BRAM: is this OK?
                    vbitrate = ic.getPropertyAsInt("b");
                }
                if (vbitrate == 0) {
                    vbitrate = 250000;
                }
                //TODO BRAM: is this OK?
                oc.setProperty("b", vbitrate);

                if (vbitratetolerance > 0) {
                    //TODO BRAM: is this OK?
                    oc.setProperty("bt", vbitratetolerance);
                }

                int oWidth = ic.getWidth();
                int oHeight = ic.getHeight();

                if (oHeight <= 0 || oWidth <= 0) {
                    throw new RuntimeException("could not find width or height in url: " + this.inputUrl);
                }

                /**
                 * For this program we don't allow the user to specify the pixel format
                 * type; we force the output to be the same as the input.
                 */
                oc.setPixelFormat(ic.getPixelFormat());

                if (vscaleFactor != 1.0) {
                    /**
                     * In this case, it looks like the output video requires rescaling, so
                     * we create a MediaPictureResampler to do that dirty work.
                     */
                    oWidth = (int) (oWidth * vscaleFactor);
                    oHeight = (int) (oHeight * vscaleFactor);

                    mVSamplers[i] = MediaPictureResampler.make(oWidth, oHeight, oc.getPixelFormat(), ic.getWidth(), ic.getHeight(), ic.getPixelFormat(), 0);
                    if (mVSamplers[i] == null) {
                        throw new RuntimeException("This version of Xuggler does not support video resampling " + i);
                    }
                }
                else {
                    mVSamplers[i] = null;
                }
                oc.setHeight(oHeight);
                oc.setWidth(oWidth);
                

                if (vquality >= 0) {
                    oc.setFlag(Coder.Flag.FLAG_QSCALE, true);
                    //TODO BRAM: I didn't find this option in Humble
                    //oc.setGlobalQuality(vquality);
                }

                /**
                 * TimeBases are important, especially for Video. In general Audio
                 * encoders will assume that any new audio happens IMMEDIATELY after any
                 * prior audio finishes playing. But for video, we need to make sure
                 * it's being output at the right rate.
                 *
                 * In this case we make sure we set the same time base as the input, and
                 * then we don't change the time stamps of any MediaPictures.
                 *
                 * But take my word that time stamps are tricky, and this only touches
                 * the envelope. The good news is, it's easier in Xuggler than some
                 * other systems.
                 */
                oc.setTimeBase(ic.getTimeBase());
                
              
                oc.setFlag(Coder.Flag.FLAG_GLOBAL_HEADER, true);

                //TODO BRAM: ok to pass nulls here?
                oc.open(null, null);
                ContainerStream os = mOContainer.addNewStream(oc);
                

                mOStreams[i] = os;
                mOCoders[i] = oc;

                /**
                 * And allocate buffers for us to store decoded and resample video
                 * pictures.
                 */
                mMediaPictures[i] = MediaPicture.make(ic.getWidth(), ic.getHeight(), ic.getPixelFormat());
                mOVideoPictures[i] = MediaPicture.make(oc.getWidth(), oc.getHeight(), oc.getPixelFormat());
            }
            else {
                log.warn("Ignoring input stream {} of type {}", i, cType);
            }

            /**
             * Now, once you've set up all the parameters on the StreamCoder, you must
             * open() them so they can do work.
             *
             * They will return an error if not configured correctly, so we check for
             * that here.
             */
            if (mOCoders[i] != null) {
                // some codecs require experimental mode to be set, and so we set it here.
                //TODO BRAM How to enable this in Humble?
                //retval = mOCoders[i].setStandardsCompliance(Coder.CodecStandardsCompliance.COMPLIANCE_EXPERIMENTAL);

                mOCoders[i].open(null, null);
                mICoders[i].open(null, null);
            }
        }

        /**
         * Pretty much every output container format has a header they need written,
         * so we do that here.
         *
         * You must configure your output ContainerStreams correctly before writing a
         * header, and few formats deal nicely with key parameters changing (e.g.
         * video width) after a header is written.
         */
        //TODO BRAM How to enable this in Humble?
        //mOContainer.writeHeader();

        mOContainer.open(null, null);

        /**
         * That's it with setup; we're good to begin!
         */
        return numStreams;
    }

    /**
     * Close and release all resources we used to run this program.
     */
    void closeStreams() throws IOException, InterruptedException
    {
        int i = 0;

        int numStreams = mIContainer.getNumStreams();
        /**
         * Some video coders (e.g. MP3) will often "read-ahead" in a stream and keep
         * extra data around to get efficient compression. But they need some way to
         * know they're never going to get more data. The convention for that case
         * is to pass null for the IMediaData (e.g. MediaAudio or MediaPicture)
         * in encodeAudio(...) or encodeVideo(...) once before closing the coder.
         *
         * In that case, the Coder will flush all data.
         */
        for (i = 0; i < numStreams; i++) {
            if (mOCoders[i] != null) {
                MediaPacket oPacket = MediaPacket.make();
                do {
                    if (mOCoders[i].getCodecType() == MediaDescriptor.Type.MEDIA_AUDIO) {
                        mOCoders[i].encodeAudio(oPacket, null);
                    }
                    else {
                        mOCoders[i].encodeVideo(oPacket, null);
                    }
                    if (oPacket.isComplete()) {
                        mOContainer.write(oPacket, mForceInterleave);
                    }
                } while (oPacket.isComplete());
            }
        }
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
        //TODO BRAM How to enable this in Humble?
        //mOContainer.writeTrailer();

        /**
         * We do a nice clean-up here to show you how you should do it.
         *
         * That said, Xuggler goes to great pains to clean up after you if you
         * forget to release things. But still, you should be a good boy or giral
         * and clean up yourself.
         */
        for (i = 0; i < numStreams; i++) {
            if (mOCoders[i] != null) {
                /**
                 * And close the input coder to tell Xuggler it can release all native
                 * memory.
                 */
                //TODO BRAM: do we call delete() here?
                //mOCoders[i].close();
            }
            mOCoders[i] = null;
            if (mICoders[i] != null) {
            /**
             * Close the input coder to tell Xuggler it can release all native
             * memory.
             */
                //TODO BRAM: do we call delete() here?
                //mICoders[i].close();
            }
            mICoders[i] = null;
        }

        /**
         * Tell Xuggler it can close the output file, write all data, and free all
         * relevant memory.
         */
        mOContainer.close();
        /**
         * And do the same with the input file.
         */
        mIContainer.close();

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
        mMediaPictures = null;
        mOVideoPictures = null;
        mOCoders = null;
        mICoders = null;
        mASamplers = null;
        mVSamplers = null;
    }

    /**
     * Allow child class to override this method to alter the audio frame before
     * it is rencoded and written. In this implementation the audio frame is
     * passed through unmodified.
     *
     * @param audioFrame the source audio frame to be modified
     * @return the modified audio frame
     */

    protected MediaAudio alterAudioFrame(MediaAudio audioFrame)
    {
        return audioFrame;
    }

    /**
     * Allow child class to override this method to alter the video frame before
     * it is rencoded and written. In this implementation the video frame is
     * passed through unmodified.
     *
     * @param videoFrame the source video frame to be modified
     * @return the modified video frame
     */

    protected MediaPicture alterVideoFrame(MediaPicture videoFrame)
    {
        return videoFrame;
    }

    /**
     * Takes a given command line and decodes the input file, and encodes with new
     * parameters to the output file.
     */
    public void run(String inputUrl, String outputUrl) throws IOException, InterruptedException
    {
        this.inputUrl = inputUrl;
        this.outputUrl = outputUrl;

        /**
         * Setup all our input and outputs
         */
        setupStreams();

        /**
         * Create packet buffers for reading data from and writing data to the
         * conatiners.
         */
        MediaPacket iPacket = MediaPacket.make();
        MediaPacket oPacket = MediaPacket.make();

        /**
         * Keep some "pointers' we'll use for the audio we're working with.
         */
        MediaAudio inSamples = null;
        MediaAudio outSamples = null;
        MediaAudio reSamples = null;

        int retval = 0;

        /**
         * And keep some convenience pointers for the specific stream we're working
         * on for a packet.
         */
        Decoder ic = null;
        Encoder oc = null;
        MediaAudioResampler as = null;
        MediaPictureResampler vs = null;
        MediaPicture inFrame = null;
        MediaPicture reFrame = null;

        /**
         * Now, we've already opened the files in #setupStreams(CommandLine). We
         * just keep reading packets from it until the Container returns <0
         */
        while (mIContainer.read(iPacket) == 0) {
            /**
             * Find out which stream this packet belongs to.
             */
            int i = iPacket.getStreamIndex();
            int offset = 0;

            /**
             * Find out if this stream has a starting timestamp
             */
            ContainerStream stream = mIContainer.getStream(i);
            long tsOffset = 0;
            if (stream.getStartTime() != Global.NO_PTS && stream.getStartTime() > 0
                && stream.getTimeBase() != null) {
                Rational defTimeBase = Rational.make(1, (int) Global.DEFAULT_PTS_PER_SECOND);
                tsOffset = defTimeBase.rescale(stream.getStartTime(), stream.getTimeBase());
            }
            /**
             * And look up the appropriate objects that are working on that stream.
             */
            ic = mICoders[i];
            oc = mOCoders[i];
            as = mASamplers[i];
            vs = mVSamplers[i];
            inFrame = mMediaPictures[i];
            reFrame = mOVideoPictures[i];
            inSamples = mISamples[i];
            reSamples = mOSamples[i];

            if (oc == null)
            // we didn't set up this coder; ignore the packet
            {
                continue;
            }

            /**
             * Find out if the stream is audio or video.
             */
            MediaDescriptor.Type cType = ic.getCodecType();

            /*if (cType == MediaDescriptor.Type.MEDIA_AUDIO && mHasAudio) {
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
                while (offset < iPacket.getSize()) {
                    retval = ic.decodeAudio(inSamples, iPacket, offset);
                    if (retval <= 0) {
                        throw new RuntimeException("could not decode audio.  stream: " + i);
                    }

                    if (inSamples.getTimeStamp() != Global.NO_PTS) {
                        inSamples.setTimeStamp(inSamples.getTimeStamp() - tsOffset);
                    }

                    log.trace("packet:{}; samples:{}; offset:{}", new Object[]
                                    {
                                                    iPacket, inSamples, tsOffset
                                    });

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
                    if (as != null && inSamples.getNumSamples() > 0) {
                        retval = as.resample(reSamples, inSamples);

                        outSamples = reSamples;
                    }
                    else {
                        outSamples = inSamples;
                    }

                    *//**
                     * Include call a hook to derivied classes to allow them to alter the
                     * audio frame.
                     *//*

                    outSamples = alterAudioFrame(outSamples);

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
                     * and we encourage you to read the MediaAudio documentation to
                     * find out what the difference is.
                     *
                     * But in any case, the following loop encodes the samples we have
                     * into packets.
                     *//*
                    while (numSamplesConsumed < outSamples.getNumSamples()) {
                        oc.encodeAudio(oPacket, outSamples);

                        *//**
                         * Increment the number of samples consumed, so that the next time
                         * through this loop we encode new audio
                         *//*
                        numSamplesConsumed += retval;
                        log.trace("out packet:{}; samples:{}; offset:{}", new Object[] {
                                        oPacket, outSamples, tsOffset
                        });

                        writePacket(oPacket);
                    }
                }

            }*/
            if (cType == MediaDescriptor.Type.MEDIA_VIDEO && mHasVideo) {
                /**
                 * This encoding workflow is pretty much the same as the for the audio
                 * above.
                 *
                 * The only major delta is that encodeVideo() will always consume one
                 * frame (whereas encodeAudio() might only consume some samples in an
                 * MediaAudio buffer); it might not be able to output a packet yet,
                 * but you can assume that you it consumes the entire frame.
                 */
                MediaPicture outFrame = null;
                while (offset < iPacket.getSize()) {
                    retval = ic.decodeVideo(inFrame, iPacket, offset);
                    if (retval <= 0) {
                        throw new RuntimeException("could not decode any video.  stream: " + i);
                    }

                    log.trace("decoded vid ts: {}; pkts ts: {}", inFrame.getTimeStamp(), iPacket.getTimeStamp());
                    if (inFrame.getTimeStamp() != Global.NO_PTS) {
                        inFrame.setTimeStamp(inFrame.getTimeStamp() - tsOffset);
                    }

                    offset += retval;
                    if (inFrame.isComplete()) {
                        if (vs != null) {
                            retval = vs.resample(reFrame, inFrame);
                            if (retval < 0) {
                                throw new RuntimeException("could not resample video");
                            }
                            outFrame = reFrame;
                        }
                        else {
                            outFrame = inFrame;
                        }

                        /**
                         * Include call a hook to derivied classes to allow them to alter
                         * the audio frame.
                         */

                        outFrame = alterVideoFrame(outFrame);

                        outFrame.setQuality(0);
                        oc.encodeVideo(oPacket, outFrame);
                        writePacket(oPacket);
                    }
                }
            }
            else {
                /**
                 * Just to be complete; there are other types of data that can show up
                 * in streams (e.g. SUB TITLE).
                 *
                 * Right now we don't support decoding and encoding that data, but youc
                 * could still decide to write out the packets if you wanted.
                 */
                log.trace("ignoring packet of type: {}", cType);
            }

        }

        // and cleanup.
        closeStreams();
    }

    private void writePacket(MediaPacket oPacket)
    {
        if (oPacket.isComplete()) {
            if (mRealTimeEncoder) {
                delayForRealTime(oPacket);
            }
            /**
             * If we got a complete packet out of the encoder, then go ahead
             * and write it to the container.
             */
            mOContainer.write(oPacket, mForceInterleave);
        }
    }

    /**
     * WARNING for those who want to copy this method and think it'll stream
     * for them -- it won't.  It doesn't interleave packets from non-interleaved
     * containers, so instead it'll write chunky data.  But it's useful if you
     * have previously interleaved data that you want to write out slowly to
     * a file, or, a socket.
     *
     * @param oPacket the packet about to be written.
     */
    private void delayForRealTime(MediaPacket oPacket)
    {
        // convert packet timestamp to microseconds
        final Rational timeBase = oPacket.getTimeBase();
        if (timeBase == null || timeBase.getNumerator() == 0 || timeBase.getDenominator() == 0) {
            return;
        }
        long dts = oPacket.getDts();
        if (dts == Global.NO_PTS) {
            return;
        }

        final long currStreamTime = Rational.rescale(dts,
                                                      1,
                                                      1000000,
                                                      timeBase.getNumerator(),
                                                      timeBase.getDenominator(),
                                                      Rational.Rounding.ROUND_NEAR_INF);
        if (mStartStreamTime == null) {
            mStartStreamTime = currStreamTime;
        }

        // convert now to microseconds
        final long currClockTime = System.nanoTime() / 1000;
        if (mStartClockTime == null) {
            mStartClockTime = currClockTime;
        }

        final long currClockDelta = currClockTime - mStartClockTime;
        if (currClockDelta < 0) {
            return;
        }
        final long currStreamDelta = currStreamTime - mStartStreamTime;
        if (currStreamDelta < 0) {
            return;
        }
        final long streamToClockDeltaMilliseconds = (currStreamDelta - currClockDelta) / 1000;
        if (streamToClockDeltaMilliseconds <= 0) {
            return;
        }
        try {
            Thread.sleep(streamToClockDeltaMilliseconds);
        }
        catch (InterruptedException e) {
        }
    }
}