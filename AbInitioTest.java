package abinitio;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.DirectoryStream;

import java.io.IOException;

import static java.lang.Math.abs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;


public class AbInitioTest {

  //'***********************************************************************************************
  //                                     Constants
  //************************************************************************************************
  private static final String USAGE_MESSAGE = "Proper Usage is: java abinitio.AbInitioTest <dir>";
  private static final int    MAX_THREADS   = 4;


  //************************************************************************************************
  //                                   Static variables
  //************************************************************************************************
  private static double averageBytesProcessed = 0f;
  private static double maxBytesProcessed     = 0f;


  //************************************************************************************************
  //                         Static inner class to represent a thread
  //************************************************************************************************
  static class AbInitioThread implements Runnable {
    private ArrayList<Path> paths          = new ArrayList<>();
    private double          skew           = 0f;
    private double          bytesProcessed = 0f;

    AbInitioThread(Path path){
      processFile(path);
    }

    double getBytesProcessed(){
      return this.bytesProcessed;
    }

    double getSkew(){
      return this.skew;
    }

    void updateSkew() {
      this.skew = (this.bytesProcessed - AbInitioTest.averageBytesProcessed) / AbInitioTest.maxBytesProcessed;
    }

    void processFile(Path path) {
      this.paths.add(path);
      this.bytesProcessed += path.toFile().length();
    }

    void unprocessFile(Path path) {
      this.paths.remove(path);
      this.bytesProcessed -= path.toFile().length();
    }

    public void run() {
      // Data processing omitted...
    }

  }


  //************************************************************************************************
  //                                   Utility methods
  //************************************************************************************************
  /**
   * Prints given usage message and terminates the program.
   */
  private static void showUsage(String usage) {
    System.out.println(usage);
    System.exit(0);
  }

  /**
   * Checks whether a valid path argument has been provided.
   * If valid, returns an ArrayList of Path objects for each of the .dat files found in the directory.
   * Otherwise, prints usage and terminates the program.
   */
  private static ArrayList<Path> checkArguments(String[] args) {
    // Check only one argument was passed in
    if(args.length != 1) {
      showUsage(USAGE_MESSAGE);
    }

    // Check path is a valid directory
    Path    path  = Paths.get(args[0]);
    boolean isDir = Files.isDirectory(path);
    if (!isDir) {
      showUsage(USAGE_MESSAGE);
    }

    // ArrayList API used over regular array, as number of files may vary
    ArrayList<Path> paths = new ArrayList<>();

    // Check directory contains at least one .dat file
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.dat")) {
      if (!stream.iterator().hasNext()) {
        showUsage(USAGE_MESSAGE);
      }
    } catch (IOException x) {
      System.err.println("Caught IOException..."); //TODO: Handle this
    }

    // Add .dat files to the ArrayList
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, "*.dat")) {
      for (Path entry: stream) {
        paths.add(entry);
      }
    } catch (IOException x) {
      System.err.println("Caught IOException..."); //TODO: Handle this
    }

    return paths;
  }

  /**
   * Prints each of the threads and their corresponding files for parrallel processsing.
   */
  private static void displayResult(ArrayList<AbInitioThread> threads){
    int id = 1;
    for (AbInitioThread thread: threads) {
      System.out.printf("Thread %d: %s%n", id, Arrays.toString(thread.paths.toArray()));
      id++;
    }
  }


  //************************************************************************************************
  //                                 Helper methods
  //************************************************************************************************
  /**
   * Calculates and returns the 'absolute skew' of a list of threads.
   * i.e. sum of the absolute values of the skews from each thread.
   */
  private static double getAbsoluteSkew(ArrayList<AbInitioThread> threads) {
    double skew = 0f;
    for (AbInitioThread thread: threads) {
      skew += abs(thread.getSkew());
    }
    return skew;
  }

  /**
   * Updates the static variables AbInitioTest.averageBytesProcessed and
   * AbInitioTest.maxBytesProcessed , based on the provided list of threads.
   */
  private static void updateBytesProcessed(ArrayList<AbInitioThread> threads) {
    double total   = 0f;
    double largest = 0f;
    double bytes;

    for (AbInitioThread thread: threads){
      bytes = thread.getBytesProcessed();
      if (bytes > largest){
        largest = bytes;
      }
      total += bytes;
    }

    AbInitioTest.averageBytesProcessed = total / threads.size();
    AbInitioTest.maxBytesProcessed     = largest;

  }

  /**
   * Updates the skews of each of the provided threads.
   */
  private static void updateSkews(ArrayList<AbInitioThread> threads) {
    for (AbInitioThread thread: threads) {
      thread.updateSkew();
    }
  }


  //************************************************************************************************
  //                                 Algorithm methods
  //************************************************************************************************

  /**
   * Brute-force algorithm for determining which thread in the provided list of threads is best
   * suited to process a given file.
   * Returns the index of the most suitable thread in the list.
   */
  private static int determineBestThread(ArrayList<AbInitioThread> threads, Path path) {
    TreeMap<Double, AbInitioThread> skewMap  = new TreeMap<>();
    AbInitioThread                  bestThread;
    double                          absoluteSkew;

    // Constructing map of (absolute skew --> thread) pairs.
    // TreeMap used because it sorts by key (i.e. by absolute skew)
    for (AbInitioThread thread: threads) {
      thread.processFile(path);
      updateBytesProcessed(threads);
      updateSkews(threads);

      absoluteSkew = getAbsoluteSkew(threads);
      skewMap.put(absoluteSkew, thread);

      thread.unprocessFile(path);
    }
    updateBytesProcessed(threads);
    updateSkews(threads);

    // Return the index of the thread that processed the file and resulted in the least absolute skew
    // i.e. the first thread in the map
    bestThread = skewMap.get(skewMap.firstKey());
    return threads.indexOf(bestThread);
  }

  /**
   * Determines how a given set of files can be most efficiently processed by a given number of threads.
   * Returns a list of AbInitioThread objects, each of which store a list of Path objects (to represent
   * the files).
   */
  private static ArrayList<AbInitioThread> determineParrallelProcessing(ArrayList<Path> paths, int maxThreads) {
    ArrayList<AbInitioThread> threads = new ArrayList<>();
    int numberOfFiles = paths.size();
    int numberOfThreads;
    int index;
    Path currentFile;

    // If few enough files provided, assign each file to its own thread and return
    if (numberOfFiles <= maxThreads) {

      numberOfThreads = numberOfFiles;

      for (int i = 0; i < numberOfThreads; i++) {
        currentFile = paths.get(i);
        AbInitioThread thread = new AbInitioThread(currentFile);
        threads.add(thread);
        // thread.start() ...   Data processing omitted
      }
      return threads;

    }
    // Otherwise, create the maximum amount of threads and determine the best thread
    // for each of the remaining files
    else {
      for (int i = 0; i < maxThreads; i++) {
        currentFile = paths.get(i);
        AbInitioThread thread = new AbInitioThread(currentFile);
        threads.add(thread);
      }
      updateBytesProcessed(threads);
      updateSkews(threads);

      // Determine best thread for each of the remaining files
      for (int i = maxThreads; i < numberOfFiles; i++) {
        currentFile = paths.get(i);
        index = determineBestThread(threads, currentFile);
        threads.get(index).processFile(currentFile);
      }

      for (AbInitioThread thread : threads) {
        // thread.start()...   Data processing omitted
      }
      return threads;
    }
  }


  //************************************************************************************************
  //                                       main
  //************************************************************************************************

  public static void main(String[] args) {
    ArrayList<Path> paths = checkArguments(args);
    ArrayList<AbInitioThread> threads = determineParrallelProcessing(paths, MAX_THREADS);
    displayResult(threads);
  }
}
