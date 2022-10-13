/*
Copyright © 2022 Will Fitzgerald <willf@github.com>
*/
package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
	"github.com/spf13/cobra"
)

// bulkCmd represents the bulk command
var bulkCmd = &cobra.Command{
	Use:   "bulk",
	Short: "Add documents to an index",
	Long: `
	Add documents to an OpenSearch index.

	Documents are read from stdin, one per line, and added to the index. Each line much be a valid JSON document.
	A document ID is required for each document. The ID field can be specified with the -f flag.
	The default ID field is _id.
	The document id and its value will be removed from the document before indexing.

	Example:
	$ cat my_documents.json | opensearch-doc bulk -i my_index -f id

	where my_documents.json is a file containing one JSON document per line:

	{"id": "1", "title": "My first document"}
	{"id": "2", "title": "My second document"}
	{"id": "3", "title": "My third document"}

	and so forth.

	`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("bulk started")
		Bulk(cmd.Flag("index").Value.String(), cmd.Flag("action").Value.String(), cmd.Flag("id_field").Value.String())
	},
}

func init() {
	rootCmd.AddCommand(bulkCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// bulkCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// bulkCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	bulkCmd.Flags().StringP("index", "i", "", "The OpenSearch index for the documents")
	// require an index flag
	bulkCmd.MarkFlagRequired("index")
	bulkCmd.Flags().StringP("id_field", "f", "_id", "The field to use as the document ID")
	bulkCmd.Flags().StringP("action", "a", "index", "What do to with the document: index, create, update, delete")
}

func Bulk(index string, action string, idField string) {
	fmt.Println("bulk called")
	// TODO: add support for other configuration options
	client, err := opensearch.NewClient(opensearch.Config{
		// Retry on 429 TooManyRequests statuses
		//
		RetryOnStatus: []int{502, 503, 504, 429},

		// A simple incremental backoff function
		//
		RetryBackoff: func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },

		// Retry up to 5 attempts
		//
		MaxRetries: 5,
	})
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	fmt.Println("client created")
	// Create the indexer
	//
	indexer, err := opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		Client:     client, // The OpenSearch client
		Index:      index,  // The default index name
		NumWorkers: 4,      // The number of worker goroutines (default: number of CPUs)
		FlushBytes: 5e+6,   // The flush threshold in bytes (default: 5M)
	})
	if err != nil {
		log.Fatalf("Error creating the indexer: %s", err)
	}
	fmt.Println("indexer created")
	scanner := bufio.NewScanner(os.Stdin)
	//for scanner.Scan() {
	//	fmt.Println(scanner.Text())
	//}

	var f interface{}
	// err := json.Unmarshal(b, &f)
	// read a JSON object from stdin
	for scanner.Scan() {
		text := scanner.Text()
		err := json.Unmarshal([]byte(text), &f)
		if err != nil {
			log.Printf("Error unmarshalling JSON: %s", err)
		}

		// get the document Id from the JSON object using the idField
		documentMap := f.(map[string]interface{})
		id := documentMap[idField]
		if id == nil {
			log.Printf("Error: document does not contain an value for the idField '%s'; not adding", idField)
			continue
		}
		// Coerce the id to a string
		idString := fmt.Sprintf("%v", id)
		// remove the id field from the JSON object
		delete(documentMap, idField)
		// marshal the JSON object back to a byte array
		document, err := json.Marshal(documentMap)
		if err != nil {
			log.Printf("Error marshalling JSON: %s", err)
		}
		// and make a string from it
		fmt.Println("indexing", idString)
		// Add an item to the indexer
		//
		err = indexer.Add(
			context.Background(),
			opensearchutil.BulkIndexerItem{
				// Action field configures the operation to perform (index, create, delete, update)
				Action: action,

				// DocumentID is the optional document ID
				DocumentID: idString,

				// Body is the document, converted to a readable byte array
				Body: strings.NewReader(string(document)),

				// OnSuccess is the optional callback for each successful operation
				OnSuccess: func(
					ctx context.Context,
					item opensearchutil.BulkIndexerItem,
					res opensearchutil.BulkIndexerResponseItem,
				) {
					fmt.Printf("[%d] %s %s\n", res.Status, res.Result, item.DocumentID)
				},

				// OnFailure is the optional callback for each failed operation
				OnFailure: func(
					ctx context.Context,
					item opensearchutil.BulkIndexerItem,
					res opensearchutil.BulkIndexerResponseItem, err error,
				) {
					if err != nil {
						log.Printf("ERROR: %s", err)
					} else {
						log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			log.Fatalf("Unexpected error: %s", err)
			fmt.Printf("Unexpected error: %s", err)
		}
	}
	// Close the indexer channel and flush remaining items
	//
	if err := indexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	// Report the indexer statistics
	//
	stats := indexer.Stats()
	if stats.NumFailed > 0 {
		log.Fatalf("Indexed [%d] documents with [%d] errors", stats.NumFlushed, stats.NumFailed)
	} else {
		log.Printf("Successfully indexed [%d] documents", stats.NumFlushed)
	}
	fmt.Printf("Indexed [%d] documents with [%d] errors\n", stats.NumFlushed, stats.NumFailed)
}
