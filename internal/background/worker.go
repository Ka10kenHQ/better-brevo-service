package background

import (
	"log"
	"os"
	"path/filepath"
	// "strings"
	// "time"
	"github.com/Ka10ken1/better-brevo-service/internal/brevo"
)



func generateTodayPath() string {
	// basePath := `C:/Users/Administrator/Desktop/winners`
	// filenamePattern := "applications_{date}_past_1days/profiles"
	basePath := "/home/achir/dev/go/better-brevo-service"
	filenamePattern := "test"
	fileExtension := ".csv"

	// date := time.Now().Format("2006-01-02")
	// filename := strings.Replace(filenamePattern, "{date}", date, 1)

	fullPath := filepath.Join(basePath, filenamePattern) + fileExtension
	return fullPath
}

func Run() {
	todayPath := generateTodayPath()

	if _, err := os.Stat(todayPath); os.IsNotExist(err) {
		log.Printf("CSV file not found: %s. Skipping this run.", todayPath)
		return
	}

	brevo.Start(todayPath)
}

