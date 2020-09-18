using Newtonsoft.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace WikiDataGraphParsing
{
    class Node
    {
        public string? id;
        public string? label; //Using Eng only

        public override string ToString()
        {
            return $"({label},{id})";
        }

        public override bool Equals(object obj)
        {
            //       
            // See the full list of guidelines at
            //   http://go.microsoft.com/fwlink/?LinkID=85237  
            // and also the guidance for operator== at
            //   http://go.microsoft.com/fwlink/?LinkId=85238
            //

            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            // TODO: write your implementation of Equals() here
            return this.GetHashCode() == obj.GetHashCode();
        }

        // override object.GetHashCode
        public override int GetHashCode()
        {
            // TODO: write your implementation of GetHashCode() here
            return id.GetHashCode();
        }
    }
    class Program
    {
        private FileStream stream;
        private StreamReader streamReader;

        private CancellationTokenSource doneProcessing;
        private ConcurrentQueue<string> workQueue;
        private ConcurrentDictionary<string, LinkedList<string>> sharedAdjacencyDict;
        private ConcurrentDictionary<string, Node> allNodes;
        private ConcurrentQueue<string> writeQueue;

        static void Main(string[] args)
        {
            string filePath = @"F:\WikiData\latest-all.json";
            if(args.Length < 1)
            {
                //Console.WriteLine("No path given, please supply a path to a single file");
                //return;
            }
            else
            {
                filePath = args.First();
            }

            Program p = new Program();
            p.runGraphGen(filePath);
        }

        public void runGraphGen(string filePath)
        {
            if (sharedAdjacencyDict == null)
            {
                sharedAdjacencyDict = new ConcurrentDictionary<string, LinkedList<string>>();
            }

            if(workQueue == null)
            {
                workQueue = new ConcurrentQueue<string>();
            }

            if(writeQueue == null)
            {
                writeQueue = new ConcurrentQueue<string>();
            }

            if(allNodes == null)
            {
                allNodes = new ConcurrentDictionary<string, Node>();
            }

            long totalSize = 0;
            if(stream == null)
            {
                totalSize = loadFile(filePath);
            }

            doneProcessing = new CancellationTokenSource();
            ThreadPool.GetAvailableThreads(out int worker, out int completion);
            int processors = Environment.ProcessorCount;
            int threadsToMake = processors; //Leave one to read and one to write
            Console.WriteLine($"Making {threadsToMake} queueThreads");
            for(int i = 0; i < threadsToMake; i++)
            {
                ThreadPool.QueueUserWorkItem(ThreadedParsing);
            }
            ThreadPool.QueueUserWorkItem(ThreadedWriting);

            Console.WriteLine($"About to start on {totalSize} bytes of data");
            var proc = System.Diagnostics.Process.GetCurrentProcess();

            long linesRead = 0;
            bool onOff = true;
            bool napped = false;
            while(!streamReader.EndOfStream)
            {
                onOff = workQueue.Count > threadsToMake * 7500;
                //onOff = proc.PrivateMemorySize64 < 
                while(onOff)
                {
                    Console.WriteLine($"Easing off due to {workQueue.Count} queued lines for {ThreadPool.ThreadCount} threads");
                    onOff = workQueue.Count < threadsToMake * 100;
                    napped = true;
                    Thread.Sleep(7500);
                }
                if(napped)
                {
                    Console.WriteLine($"Starting back up again with {workQueue.Count} lines in buffer");
                    napped = false;
                }
                
                string line = streamReader.ReadLine();
                //Remove trailing ,
                line = line.Remove(line.Length - 1, 1);
                workQueue.Enqueue(line);
                linesRead++;

                if(linesRead % 50000 == 0)
                {
                    double pctDone = Math.Round(((double)stream.Position / (double)totalSize)*100.0,5);
                    Console.WriteLine($"{pctDone}% complete");
                }
            }
            doneProcessing.Cancel();

            /*
            //Save result to disk
            FileStream fs = new FileStream("adjacency.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            StreamWriter sw = new StreamWriter(fs);
            StringBuilder sb = new StringBuilder();
            foreach (var key in sharedAdjacencyDict.Keys)
            {
                string list = "[" + String.Join(',', sharedAdjacencyDict[key]) + "]";
                sw.WriteLine($"{sharedAdjacencyDict[key]}: {list}");
                sb.AppendLine($"{sharedAdjacencyDict[key]}: {list}");
            }
            Console.WriteLine(sb.ToString());
            sw.Flush();
            sw.Close();
            */
        }

        public long loadFile(string path)
        {
            Console.WriteLine("Loading: " + path);
            try
            {
                stream = new FileStream(path, FileMode.Open, FileAccess.Read);
                streamReader = new StreamReader(stream);
                streamReader.ReadLine(); //Skip irrelevant "["
            }
            catch(IOException e)
            {
                Console.WriteLine("Failed to load the file");
                throw e;
            }
            finally
            {
                Console.WriteLine("Loaded: " + stream.Name);
            }
            return stream.Length;
        }

        public void ThreadedWriting(Object stateInfo) //stateInfo is null
        {
            //Save result to disk
            FileStream fs = new FileStream("adjacency.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite);
            StreamWriter sw = new StreamWriter(fs);

            while (!(doneProcessing.Token.IsCancellationRequested && writeQueue.IsEmpty))
            {
                string line;
                if (!writeQueue.TryDequeue(out line))
                {
                    Thread.Sleep(5000);
                    continue;
                }

                sw.WriteLine(line);
            }

            sw.Flush();
            sw.Close();
        }

        public void ThreadedParsing(Object stateInfo) //stateInfo is null
        {
            while(!(doneProcessing.Token.IsCancellationRequested && workQueue.IsEmpty))
            {
                string line;
                if(!workQueue.TryDequeue(out line))
                {
                    Thread.Sleep(5000);
                    continue;
                }

                string entry = generateNode(line);
                if(entry != String.Empty)
                {
                    writeQueue.Enqueue(entry);
                }
                /*
                if(n != null)
                {
                    bool succ = allNodes.TryAdd(n.id, n);
                    if (!succ)
                    {
                        Console.WriteLine("Node overlap! Problem");
                    }
                }
                */
            }
        }

        public string generateNode(string line)
        {           
            //Console.WriteLine(line);
            try
            {
                //Double parsing is inefficient, but ease of use is considered. May change
                var content = JToken.Parse(line);
                //dynamic content = JsonConvert.DeserializeObject(line);
                //Console.WriteLine(content.ToString());
                Node n = new Node();
                n.id = content["id"].ToString();
                if(content["labels"]["en"] != null)
                {
                    n.label = content["labels"]["en"]["value"].ToString();
                } else
                {
                    return String.Empty; //English only for now
                    //n.label = content["labels"].Children().First().First()["value"].ToString();
                    //Console.WriteLine($"Non-Standard name: {n.label}");
                }
                if (content["type"].ToString() != "item" || n.label == null)
                {
                    //Console.WriteLine($"Skipping non-item: {n.label}");
                    return String.Empty;
                }

                var claims = content["claims"];
                /*
                if(!sharedAdjacencyDict.ContainsKey(n.id))
                { //Initial setup for that entry
                    bool succ = sharedAdjacencyDict.TryAdd(n.id, new LinkedList<string>());
                    if(!succ)
                    {
                        Console.WriteLine("Overlap, problem!");
                    }
                } */
                var links = new LinkedList<string>();
                foreach(var claim in claims.Children())
                {
                    //Console.WriteLine($"Claim: {claim}");
                    if(claim.Children().Count() > 0)
                    {
                        foreach(var snak in claim.Children())
                        {
                            //Console.WriteLine($"Snak: {snak}");
                            foreach(var childsnak in snak.Children())
                            {
                                if (childsnak["mainsnak"]["snaktype"].ToString() != "value")
                                    continue; //Skip non-values as they won't have datavalues

                                string snakdatatype = childsnak["mainsnak"]["datatype"].ToString();
                                if (snakdatatype != "wikibase-item")
                                    continue;

                                //Console.WriteLine($"ChildSnak: {childsnak}");
                                string linkID = childsnak["mainsnak"]["datavalue"]["value"]["id"].ToString();
                                //sharedAdjacencyDict[n.id].AddLast(linkID);
                                links.AddLast(linkID);
                            } 
                        }
                    }
                }

                return $"{n}:[{String.Join(',',links)}]";
            }
            catch(IOException e)
            {
                Console.WriteLine("IOException: " + e.Message);
                return String.Empty;
            }

        }
    }
}
