package main

import (
	"log"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	_ "github.com/mattn/go-sqlite3"
)

var (
	store *Store
)

func main() {
	var err error

	store, err = NewStore()
	if err != nil {
		log.Fatal(err)
	}

	app := fiber.New(fiber.Config{
		ServerHeader:                 "fasql/v0.0.1",
		IdleTimeout:                  time.Second * 3,
		CaseSensitive:                true,
		StrictRouting:                true,
		DisablePreParseMultipartForm: true,
		DisableDefaultDate:           true,
		DisableStartupMessage:        true,
	})

	app.Use(cors.New())
	app.Use(recover.New())
	app.Use(compress.New())

	app.All("/", func(c *fiber.Ctx) error {
		return c.JSON(OkResponse{
			Success: true,
		})
	})

	app.Post("/query/:dbname/write", func(c *fiber.Ctx) error {
		var req QueryRequest

		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(ErrorResponse{
				Success: false,
				Error:   err.Error(),
			})
		}

		if len(strings.TrimSpace(req.Query)) < 1 {
			return c.Status(400).JSON(ErrorResponse{
				Success: false,
				Error:   "empty query specified",
			})
		}

		result, err := store.Write(c.Params("dbname"), req)
		if err != nil {
			return c.Status(500).JSON(ErrorResponse{
				Success: false,
				Error:   err.Error(),
			})
		}

		return c.JSON(WriteQueryResponse{
			Success: true,
			Result:  result,
		})
	})

	app.Post("/query/:dbname/read", func(c *fiber.Ctx) error {
		var req QueryRequest

		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).JSON(ErrorResponse{
				Success: false,
				Error:   err.Error(),
			})
		}

		if len(strings.TrimSpace(req.Query)) < 1 {
			return c.Status(400).JSON(ErrorResponse{
				Success: false,
				Error:   "empty query specified",
			})
		}

		result, err := store.Read(c.Params("dbname"), req)
		if err != nil {
			return c.Status(500).JSON(ErrorResponse{
				Success: false,
				Error:   err.Error(),
			})
		}

		return c.JSON(ReadQueryResponse{
			Success: true,
			Result:  result,
		})
	})

	log.Fatal(app.Listen(":6000"))
}
