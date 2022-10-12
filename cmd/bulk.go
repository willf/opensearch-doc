/*
Copyright © 2022 Will Fitzgerald <willf@github.com>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// bulkCmd represents the bulk command
var bulkCmd = &cobra.Command{
	Use:   "bulk",
	Short: "Add documents to an index",
	Long:  `Add documents to an opensearch index.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("bulk called")
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
}
