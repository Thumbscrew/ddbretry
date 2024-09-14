package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name: "go-template",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "msg",
				Aliases:  []string{"m"},
				Required: false,
				Value:    "world",
				Usage:    "say hello",
			},
		},
		Action: func(ctx *cli.Context) error {
			msg := ctx.String("msg")
			fmt.Printf("hello %s\n", msg)

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Print("fatal: ", err)
		os.Exit(1)
	}
}
