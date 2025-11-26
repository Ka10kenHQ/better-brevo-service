package brevo

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const FolderUrl string = "https://api.brevo.com/v3/contacts/folders"

type Config = struct {
	APIKey      string
	SenderName  string
	SenderEmail string
}

type CSVData struct {
	NAT        string `json:"nat"`
	STOP       string `json:"stop"`
	CATEGORY   string `json:"category"`
	ID         string `json:"id"`
	Contacts   string `json:"contacts"`
	Email      string `json:"email"`
	Website    string `json:"website"`
	VendorName string `json:"vendor_name"`
	Address    string `json:"address"`
	IdCode     string `json:"id_code"`
	Phone      string `json:"phone"`
	Fax        string `json:"fax"`
	City       string `json:"city"`
	Country    string `json:"country"`
}

type BrevoContact struct {
	ID                int                    `json:"id"`
	Email             string                 `json:"email"`
	EmailBlacklisted  bool                   `json:"emailBlacklisted"`
	SMSBlacklisted    bool                   `json:"smsBlacklisted"`
	CreatedAt         string                 `json:"createdAt"`
	ModifiedAt        string                 `json:"modifiedAt"`
	ListIds           []int                  `json:"listIds"`
	Attributes        map[string]any         `json:"attributes"`
}


type BrevoService struct {
	config Config
	httpClient *http.Client
}

type ContactsResponse struct {
	Contacts []BrevoContact  `json:"contacts"`
	Count    int             `json:"count"`
}

type Folder struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type FoldersResponse struct {
	Folders []Folder `json:"folders"`
	Count   int      `json:"count"`
}

type ContantPayload struct {
	Email         string                 `json:"email"`
	UpdateEnabled bool                   `json:"updateEnabled"`
	Attributes    map[string]any         `json:"attributes,omitempty"`
	ListIds       []int                  `json:"listIds,omitempty"`
}

type ContactPayload struct {
	Email         string                 `json:"email"`
	UpdateEnabled bool                   `json:"updateEnabled"`
	Attributes    map[string]any         `json:"attributes,omitempty"`
	ListIds       []int                  `json:"listIds,omitempty"`
}

type CampaignPayload struct {
	Sender      map[string]string `json:"sender"`
	Name        string            `json:"name"`
	Subject     string            `json:"subject"`
	HTMLContent string            `json:"htmlContent"`
	Recipients  map[string][]int  `json:"recipients"`
}

type CampaignResult struct {
	Success      bool   `json:"success"`
	CampaignID   int    `json:"campaign_id,omitempty"`
	CampaignName string `json:"campaign_name,omitempty"`
	StatusCode   int    `json:"status_code"`
	Error        string `json:"error,omitempty"`
}

type SendCampaignResult struct {
	Success    bool   `json:"success"`
	Message    string `json:"message,omitempty"`
	StatusCode int    `json:"status_code"`
	Error      string `json:"error,omitempty"`
}

type ProcessingResults struct {
	AddedToCampaign        []ContactResult `json:"added_to_campaign"`
	UpdatedContacts        []ContactResult `json:"updated_contacts"`
	Errors                 []ErrorResult   `json:"errors"`
	CampaignInfo           CampaignResult  `json:"campaign_info"`
	TotalExistingContacts  int             `json:"total_existing_contacts"`
}

type ContactResult struct {
	Email  string   `json:"email"`
	Data   *CSVData `json:"data"`
	Action string   `json:"action,omitempty"`
}

type ErrorResult struct {
	Email   string `json:"email,omitempty"`
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}



func NewBrevoService() (*BrevoService, error) {
	err := godotenv.Load()

	if err != nil {
		log.Printf("Warning: Could not load .env file: %v. Falling back to system environment variables.", err)
	}

	config := Config {
		APIKey:      os.Getenv("BREVO_API_KEY"),
		SenderName:  os.Getenv("SENDER_NAME"),
		SenderEmail: os.Getenv("SENDER_EMAIL"),
	}

	if config.APIKey == "" || config.SenderName == "" || config.SenderEmail == "" {
		return nil, fmt.Errorf("missing required environment variables: BREVO_API_KEY, SENDER_NAME, SENDER_EMAIL")
	}

	return &BrevoService{
		config : config,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}


func (b *BrevoService) makeAPIRequest(method, url string, payload any) (*http.Response, error) {
	var reqBody io.Reader

	if payload != nil {
		jsonData, err := json.Marshal(payload)

		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}

		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequest(method, url, reqBody)

	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("api-key", b.config.APIKey)
	req.Header.Set("accept", "application/json")
	req.Header.Set("content-type", "application/json")

	return b.httpClient.Do(req)
}

func (b *BrevoService) GetExistingContantsEmail() (map[string]bool, error) {
	allContacts := make(map[string]bool)
	offset := 0
	limit := 1000

	log.Println("Starting to fetch all existing contacts...")

	for {
		url := fmt.Sprintf("https://api.brevo.com/v3/contacts?limit=%d&offset=%d", limit, offset)

		resp, err := b.makeAPIRequest("GET", url, nil)

		if err != nil {
			return nil, fmt.Errorf("error fetching contacts at offset %d: %w", offset, err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("API error at offset %d: %d", offset, resp.StatusCode)
		}

		var contactsResp ContactsResponse

		if err := json.NewDecoder(resp.Body).Decode(&contactsResp); err != nil {
			return nil, fmt.Errorf("failed to decode response: %w", err)
		}

		if len(contactsResp.Contacts) == 0 {
			break
		}

		for _, contact := range contactsResp.Contacts {
			if contact.Email != "" {
				allContacts[strings.ToLower(contact.Email)] = true
			}
		}

		log.Printf("Fetched %d contacts (offset: %d). Total so far: %d", len(contactsResp.Contacts), offset, len(allContacts))

		if len(contactsResp.Contacts) < limit {
			break
		}

		offset += limit
		time.Sleep(100 * time.Millisecond) // rate limiting
	}

	log.Printf("Finished fetching contacts. Total: %d unique emails found", len(allContacts))
	return allContacts, nil
}


func (b *BrevoService) GetOrCreateFolder(name string) (int, error) {
	resp, err := b.makeAPIRequest("GET", FolderUrl, nil)

	if err != nil {
		return 0, fmt.Errorf("error checking existing folders: %w", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	if err != nil {
		return 0, fmt.Errorf("failed to read folders response body: %w", err)
	}

	log.Printf("Folders API response: %d - %s", resp.StatusCode, string(body))

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("failed to fetch folders: status %d - %s", resp.StatusCode, string(body))
	}

	var folderResp FoldersResponse
	if err := json.Unmarshal(body, &folderResp); err != nil {
		log.Printf("Failed to decode folders response: %v", err)
	}

	for _, folder := range folderResp.Folders {
		if folder.Name == name {
			if folder.ID <= 0 {
				return 0, fmt.Errorf("invalid folder ID %d for folder '%s'", folder.ID, name)
			}
			log.Printf("Found existing folder '%s' with ID: %d", name, folder.ID)
			return folder.ID, nil
		}
	}

	log.Printf("Folder '%s' not found. Creating new one...", name)

	return b.CreateFolder(name)
} 


func (b *BrevoService) CreateFolder(name string) (int, error) {
	payload := map[string]string{"name": name}

	resp, err := b.makeAPIRequest("POST", FolderUrl , payload)

	if err != nil {
		return 0, fmt.Errorf("exception creating folder '%s': %w", name, err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read folder creation response body: %w", err)
	}

	log.Printf("Create Folder API response: %d - %s", resp.StatusCode, string(body))

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return 0, fmt.Errorf("failed to create folder '%s': status %d - %s", name, resp.StatusCode, string(body))
	}

	var result map[string]any

	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("failed to decode folder creation response: %w", err)
	}

	folderID, ok := result["id"].(float64)

	if !ok || folderID <= 0 {
		return 0, fmt.Errorf("invalid or missing folder ID in response: %v", result)
	}

	log.Printf("Created new folder '%s' with ID: %d", name, int(folderID))
	return int(folderID), nil
}


func (b *BrevoService) AddContact(email string, existingContacts map[string]bool, listIDs []int, contactData *CSVData) (*http.Response, error) {
	if b.config.APIKey == "" {
		return nil, fmt.Errorf("BREVO_API_KEY is not configured in environment variables")
	}

	log.Printf("users list: %d contacts found", len(existingContacts))

	contactExists := existingContacts[strings.ToLower(email)]

	if contactExists {
		log.Printf("[-] %s already exists. Will update with new data if provided.", email)
	}

	payload := b.buildPayload(email, listIDs, contactData)

	return b.sendContactPayload(email, payload, contactExists)
}


func (b *BrevoService) buildPayload(email string, listIDs []int, contactData *CSVData) ContactPayload {

	payload := ContactPayload {
		Email:         email,
		UpdateEnabled: true,
	}

	attributes := b.buildAttributes(contactData)
	if len(attributes) > 0 {
		payload.Attributes = attributes
		log.Printf("Adding contact with attributes: %v", attributes)
	} else {
		log.Println("No attributes to add - contact_data was empty or had no valid fields")
	}

	if len(listIDs) > 0 {
		payload.ListIds = listIDs
	}

	return payload
}

func (b *BrevoService) buildAttributes(contactData *CSVData) map[string]any {
	if contactData == nil {
		return map[string]any{}
	}

	attributes := make(map[string]any)
	fieldMapping := map[string]string{
		"VendorName": "COMPANY_NAME",
		"IdCode":     "COMPANY_ID", 
		"Phone":      "SMS",
		"CATEGORY":   "TENDER_CODE",
	}

	dataMap := map[string]string{
		"VendorName": contactData.VendorName,
		"IdCode":     contactData.IdCode,
		"Phone":      contactData.Phone,
		"CATEGORY":   contactData.CATEGORY,
	}

	for key, value := range dataMap {
		if value != "" && value != "http://" {
			if brevoField, exists := fieldMapping[key]; exists {
				attributes[brevoField] = value
			}
		}
	}

	return attributes
}

func (b *BrevoService) sendContactPayload(email string, payload ContactPayload, contactExists bool) (*http.Response, error) {
	url := "https://api.brevo.com/v3/contacts"
	resp, err := b.makeAPIRequest("POST", url, payload)
	if err != nil {
		log.Printf("Exception occurred while contacting Brevo API for %s: %v", email, err)
		return nil, err
	}

	body, _ := io.ReadAll(resp.Body)
	log.Printf("Brevo API response: %d - %s", resp.StatusCode, string(body))

	if b.isDuplicateSMSError(resp, string(body)) {
		return b.retryWithoutSMS(email, payload)
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		log.Printf("Failed to add/update contact %s: %d %s", email, resp.StatusCode, string(body))
	} else {
		action := "Updated"
		if !contactExists {
			action = "Added"
		}
		log.Printf("%s contact %s with additional data", action, email)
	}

	return resp, nil
}

func (b *BrevoService) isDuplicateSMSError(resp *http.Response, body string) bool {
	return resp.StatusCode == http.StatusBadRequest && 
	strings.Contains(body, "SMS is already associated with another Contact")
}

func (b *BrevoService) LoadHTMLTemplate(filename string) (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)

	if !ok {
		return "", fmt.Errorf("cannot get current file info")
	}

	currentDir := filepath.Dir(currentFile)

	path := filepath.Join(currentDir, "..", "..", "static", filename)

	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}


func (b *BrevoService) CreateNewCampaign(listID int) CampaignResult {
	htmlContent, err := b.LoadHTMLTemplate("message_template.html")
	if err != nil {
		return CampaignResult{
			Success:    false,
			Error:      fmt.Sprintf("Failed to load HTML template: %v", err),
			StatusCode: 0,
		}
	}

	timestamp := time.Now().Unix()
	campaignName := fmt.Sprintf("CSV Import Campaign - %d", timestamp)

	payload := CampaignPayload{
		Sender: map[string]string{
			"name":  b.config.SenderName,
			"email": b.config.SenderEmail,
		},
		Name:        campaignName,
		Subject:     "დოკუმენტაციის თარგმნა ნოტარიულად დამოწმებით",
		HTMLContent: htmlContent,
		Recipients: map[string][]int{
			"listIds": {listID},
		},
	}

	url := "https://api.brevo.com/v3/emailCampaigns"

	resp, err := b.makeAPIRequest("POST", url, payload)

	if err != nil {
		return CampaignResult{
			Success:    false,
			Error:      fmt.Sprintf("Exception: %v", err),
			StatusCode: 0,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusAccepted {
		var result map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return CampaignResult{
				Success:    false,
				Error:      fmt.Sprintf("Failed to decode response: %v", err),
				StatusCode: resp.StatusCode,
			}
		}

		campaignID, ok := result["id"].(float64)
		if !ok {
			return CampaignResult{
				Success:    false,
				Error:      "Invalid campaign ID in response",
				StatusCode: resp.StatusCode,
			}
		}

		log.Printf("Campaign '%s' created successfully with ID: %d", campaignName, int(campaignID))
		return CampaignResult{
			Success:      true,
			CampaignID:   int(campaignID),
			CampaignName: campaignName,
			StatusCode:   resp.StatusCode,
		}
	}

	body, _ := io.ReadAll(resp.Body)
	return CampaignResult{
		Success:    false,
		Error:      fmt.Sprintf("API Error: %d - %s", resp.StatusCode, string(body)),
		StatusCode: resp.StatusCode,
	}
}


func (b *BrevoService) SendCampaignToContacts(campaignID int) SendCampaignResult {
	url := fmt.Sprintf("https://api.brevo.com/v3/emailCampaigns/%d/sendNow", campaignID)

	resp, err := b.makeAPIRequest("POST", url, nil)
	if err != nil {
		return SendCampaignResult{
			Success:    false,
			Error:      fmt.Sprintf("Exception: %v", err),
			StatusCode: 0,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent {
		log.Printf("Campaign %d sent successfully", campaignID)
		return SendCampaignResult{
			Success:    true,
			Message:    fmt.Sprintf("Campaign %d sent to all contacts", campaignID),
			StatusCode: resp.StatusCode,
		}
	}

	body, _ := io.ReadAll(resp.Body)
	log.Printf("Failed to send campaign %d: %d %s", campaignID, resp.StatusCode, string(body))
	return SendCampaignResult{
		Success:    false,
		Error:      fmt.Sprintf("Send failed: %d - %s", resp.StatusCode, string(body)),
		StatusCode: resp.StatusCode,
	}
}

func (b *BrevoService) retryWithoutSMS(email string, payload ContactPayload) (*http.Response, error) {
	log.Printf("SMS already exists for another contact. Retrying %s without SMS field...", email)

	newAttributes := make(map[string]any)
	for k, v := range payload.Attributes {
		if k != "SMS" {
			newAttributes[k] = v
		}
	}

	payloadWithoutSMS := payload
	payloadWithoutSMS.Attributes = newAttributes

	url := "https://api.brevo.com/v3/contacts"

	if len(newAttributes) > 0 {
		log.Printf("Retrying with payload: %v", payloadWithoutSMS)
		resp, err := b.makeAPIRequest("POST", url, payloadWithoutSMS)
		if err != nil {
			return nil, err
		}

		body, _ := io.ReadAll(resp.Body)
		log.Printf("Retry without SMS - Brevo API response: %d - %s", resp.StatusCode, string(body))
		return resp, nil
	} else {
		log.Printf("No other attributes to update for %s, treating as success", email)
		return &http.Response{StatusCode: http.StatusNoContent}, nil
	}
}

func (b *BrevoService) CreateNewContactList(csvName string) (int, error) {
	folderID, err := b.GetOrCreateFolder("Winners")

	if err != nil {
		return 0, fmt.Errorf("failed to get or create folder for contact lists: %w", err)
	}

	if folderID <= 0 {
		return 0, fmt.Errorf("invalid folder ID %d for contact list creation", folderID)
	}

	now := time.Now().Format("2006-01-02 15:04:05")
	payload := map[string]any{
		"name":     fmt.Sprintf("Winners List - %s", now),
		"folderId": folderID,
	}

	url := "https://api.brevo.com/v3/contacts/lists"

	resp, err := b.makeAPIRequest("POST", url , payload)

	if err != nil {
		return 0, fmt.Errorf("exception creating contact list: %w", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read contact list creation response body: %w", err)
	}

	log.Printf("Create Contact List API response: %d - %s", resp.StatusCode, string(body))

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return 0, fmt.Errorf("failed to create contact list: status %d - %s", resp.StatusCode, string(body))
	}

	var result map[string]any

	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("failed to decode list creation response: %w", err)
	}

	listID, ok := result["id"].(float64)

	if !ok || listID <= 0 {
		return 0, fmt.Errorf("invalid or missing list ID in response: %v", result)
	}

	log.Printf("Created new contact list with ID: %d", int(listID))
	return int(listID), nil
}

func mapCSVToObject(records [][]string) ([]CSVData, error) {
	if len(records) < 2 {
		return nil, fmt.Errorf("CSV file is empty or has no data rows")
	}

	expectedColumns := 14
	data := make([]CSVData, 0, len(records)-1)

	for i, row := range records[1:] { 
		if len(row) != expectedColumns {
			return nil, fmt.Errorf("row %d has %d columns, expected %d", i+1, len(row), expectedColumns)
		}

		data = append(data, CSVData {
			NAT:        row[0],
			STOP:       row[1],
			CATEGORY:   row[2],
			ID:         row[3],
			Contacts:   row[4],
			Email:      row[5],
			Website:    row[6],
			VendorName: row[7],
			Address:    row[8],
			IdCode:     row[9],
			Phone:      row[10],
			Fax:        row[11],
			City:       row[12],
			Country:    row[13],
		})
	}

	return data, nil
}

func (b *BrevoService) ProcessCSVAndSendCampaign(csvPath string) (ProcessingResults, error) {
	results := ProcessingResults{
		AddedToCampaign:   []ContactResult{},
		UpdatedContacts:   []ContactResult{},
		Errors:            []ErrorResult{},
		TotalExistingContacts: 0,
	}

	file, err := os.Open(csvPath)

	if err != nil {
		return results, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()

	if err != nil {
		return results, fmt.Errorf("failed to read CSV: %w", err)
	}

	csvData, err := mapCSVToObject(records)

	if err != nil {
		return results, fmt.Errorf("failed to map CSV data: %w", err)
	}

	existingContacts, err := b.GetExistingContantsEmail()

	if err != nil {
		return results, fmt.Errorf("failed to fetch existing contacts: %w", err)
	}

	results.TotalExistingContacts = len(existingContacts)

	csvName := strings.TrimSuffix(filepath.Base(csvPath), ".csv")

	listID, err := b.CreateNewContactList(csvName)

	if err != nil {
		return results, fmt.Errorf("failed to create contact list: %w", err)
	}

	for _, data := range csvData {
		if data.Email == "" {
			results.Errors = append(results.Errors, ErrorResult{
				Email:  data.Email,
				Error:  "missing email",
				Details: "Skipping contact with no email address",
			})
			continue
		}

		_ , err := b.AddContact(data.Email, existingContacts, []int{listID}, &data)
		if err != nil {
			results.Errors = append(results.Errors, ErrorResult{
				Email:  data.Email,
				Error:  err.Error(),
				Details: "Failed to add/update contact",
			})
			continue
		}

		contactResult := ContactResult{
			Email: data.Email,
			Data:  &data,
		}

		if existingContacts[strings.ToLower(data.Email)] {
			contactResult.Action = "Updated"
			results.UpdatedContacts = append(results.UpdatedContacts, contactResult)
		} else {
			contactResult.Action = "Added"
			results.AddedToCampaign = append(results.AddedToCampaign, contactResult)
		}
	}

	campaignResult := b.CreateNewCampaign(listID)
	results.CampaignInfo = campaignResult
	if !campaignResult.Success {
		results.Errors = append(results.Errors, ErrorResult{
			Error:  campaignResult.Error,
			Details: "Failed to create campaign",
		})
		return results, nil
	}

	sendResult := b.SendCampaignToContacts(campaignResult.CampaignID)
	if !sendResult.Success {
		results.Errors = append(results.Errors, ErrorResult{
			Error:  sendResult.Error,
			Details: "Failed to send campaign",
		})
	}

	return results, nil
}


func Start(csvPath string) {
	service, err := NewBrevoService()
	if err != nil {
		log.Fatalf("Failed to initialize Brevo service: %v", err)
	}

	results, err := service.ProcessCSVAndSendCampaign(csvPath)
	if err != nil {
		log.Printf("Failed to process CSV and send campaign: %v", err)
		return
	}

	log.Printf("Processing Results:")
	log.Printf("Total Existing Contacts: %d", results.TotalExistingContacts)
	log.Printf("Added Contacts: %d", len(results.AddedToCampaign))
	log.Printf("Updated Contacts: %d", len(results.UpdatedContacts))
	log.Printf("Errors: %d", len(results.Errors))
	log.Printf("Campaign: %s (ID: %d, Success: %v)", 
		results.CampaignInfo.CampaignName, 
		results.CampaignInfo.CampaignID, 
		results.CampaignInfo.Success)

	for _, errResult := range results.Errors {
		log.Printf("Error: %s (%s)", errResult.Error, errResult.Details)
	}
}

