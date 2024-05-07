package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Ticket struct {
	TicketID           string       `json:"ticket_id"`
	PlacedOn           time.Time    `json:"placed_on"`
	PlayerID           string       `json:"player_id"`
	Channel            ChannelType  `json:"channel"`
	ServicePoint       ServicePoint `json:"service_point"`
	Location           Location     `json:"location"`
	DeviceID           string       `json:"device_id"`
	CashierID          *string      `json:"cashier_id,omitempty"`
	Stake              Money        `json:"stake"`
	StakeType          StakeType    `json:"stake_type"`
	Currency           interface{}  `json:"currency"` // Should be validated against a list of allowed currencies
	BaseStake          Money        `json:"base_stake"`
	Lines              []Line       `json:"lines"`
	TicketOdd          *Decimal     `json:"ticket_odd,omitempty"`
	PotentialWinning   *Money       `json:"potential_winning,omitempty"`
	Bonus              []Bonus      `json:"bonus"`
	SettlementStatus   Settlement   `json:"settlement_status"`
	SettlementDatetime time.Time    `json:"settlement_datetime"`
	PaymentStatus      Payment      `json:"payment_status"`
	Payout             Money        `json:"payout"`
}

type ChannelType string

const (
	Online ChannelType = "Online"
	Retail ChannelType = "Retail"
)

type ServicePoint string

const (
	Desktop  ServicePoint = "Desktop"
	Tablet   ServicePoint = "Tablet"
	Mobile   ServicePoint = "Mobile"
	Terminal ServicePoint = "Terminal"
	Cashier  ServicePoint = "Cashier"
)

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type StakeType string

type Line struct {
	LineID         string      `json:"line_id"`
	EventID        string      `json:"event_id"`
	EventType      EventType   `json:"event_type"`
	StartAt        time.Time   `json:"start_at"`
	StopAt         *time.Time  `json:"stop_at,omitempty"`
	EventName      string      `json:"event_name"`
	HomeCompetitor Competitor  `json:"home_competitor"`
	AwayCompetitor Competitor  `json:"away_competitor"`
	Venue          interface{} `json:"venue"` // Should be validated against a list of allowed venues
	Selection      []Selection `json:"selection"`
	LineOdd        Decimal     `json:"line_odd"`
}

type EventType string

const (
	Match    EventType = "Match"
	Race     EventType = "Race"
	Outright EventType = "Outright"
)

type Competitor struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Selection struct {
	MarketID           *string    `json:"market_id,omitempty"`
	OutcomeID          string     `json:"outcome_id"`
	Odd                Decimal    `json:"odd"`
	SettlementStatus   Settlement `json:"settlement_status"`
	StakeFactor        Decimal    `json:"stake_factor"`
	OddFactor          Decimal    `json:"odd_factor"`
	SettlementDatetime time.Time  `json:"settlement_datetime"`
}

type Settlement string

const (
	Won      Settlement = "Won"
	Lost     Settlement = "Lost"
	HalfWon  Settlement = "Half-won"
	HalfLost Settlement = "Half-lost"
	Void     Settlement = "Void"
)

type Decimal float64

type Money float64

type Bonus struct {
	Type                    BonusType   `json:"type"`
	TypeDefinition          interface{} `json:"type_definition"` // Should be a structured JSON
	RewardType              RewardType  `json:"reward_type"`
	RewardValue             Decimal     `json:"reward_value"`
	BonusSettlement         Settlement  `json:"bonus_settlement"`
	BonusSettlementDatetime time.Time   `json:"bonus_settlement_datetime"`
}

type BonusType string

const (
	Boost  BonusType = "Boost"
	Refund BonusType = "Refund"
)

type RewardType string

const (
	Freebet    RewardType = "Freebet"
	Voucher    RewardType = "Voucher"
	Cash       RewardType = "Cash"
	BonusMoney RewardType = "BonusMoney"
	Spin       RewardType = "Spin"
	Try        RewardType = "Try"
)

type Payment string

const (
	Yes     Payment = "Yes"
	No      Payment = "No"
	Expired Payment = "Expired"
)

// GenerateRandomTicket generates a random Ticket instance.
func (tg *TicketGenerator) GenerateRandomTicket() Ticket {
	rand.Seed(time.Now().UnixNano())

	// Generate random ticket details
	ticketID := generateRandomGUID()
	placedOn := generateRandomDateTime()
	playerID := generateRandomGUID()
	channel := ChannelType(rand.Intn(2))                                                         // Randomly choose Online or Retail
	servicePoint := ServicePoint(rand.Intn(5))                                                   // Randomly choose from available service points
	location := Location{Latitude: rand.Float64()*180 - 90, Longitude: rand.Float64()*360 - 180} // Random location
	deviceID := generateRandomGUID()
	stake := Money(rand.Float64() * 1000)                    // Random stake amount up to 1000
	stakeType := StakeType(rand.Intn(4))                     // Randomly choose from available stake types
	currency := tg.currencies[rand.Intn(len(tg.currencies))] // Randomly choose currency
	baseStake := Money(rand.Float64() * 1000)                // Random base stake amount up to 1000
	lines := tg.generateRandomLines()
	settlementStatus := Settlement(rand.Intn(5)) // Randomly choose from available settlement statuses
	settlementDatetime := generateRandomDateTime()
	paymentStatus := Payment(rand.Intn(3)) // Randomly choose from available payment statuses
	payout := Money(rand.Float64() * 1000) // Random payout amount up to 1000

	// Construct Ticket instance
	ticket := Ticket{
		TicketID:           ticketID,
		PlacedOn:           placedOn,
		PlayerID:           playerID,
		Channel:            channel,
		ServicePoint:       servicePoint,
		Location:           location,
		DeviceID:           deviceID,
		Stake:              stake,
		StakeType:          stakeType,
		Currency:           currency,
		BaseStake:          baseStake,
		Lines:              lines,
		SettlementStatus:   settlementStatus,
		SettlementDatetime: settlementDatetime,
		PaymentStatus:      paymentStatus,
		Payout:             payout,
	}

	return ticket
}

// TicketGenerator is a struct responsible for generating random Ticket instances.
type TicketGenerator struct {
	venues     []string
	currencies []string
}

// NewTicketGenerator creates a new TicketGenerator instance.
func NewTicketGenerator() *TicketGenerator {
	return &TicketGenerator{
		venues:     []string{"Stadium A", "Arena B", "Track C", "Venue X"},
		currencies: []string{"EUR", "USD", "GBP"},
	}
}

// generateRandomLines generates a random number of lines for a Ticket.
func (tg *TicketGenerator) generateRandomLines() []Line {
	numLines := rand.Intn(5) + 1 // Generate random number of lines between 1 and 5

	lines := make([]Line, numLines)
	for i := 0; i < numLines; i++ {
		lineID := generateRandomGUID()
		eventID := generateRandomGUID()
		eventType := EventType(rand.Intn(3)) // Randomly choose from available event types
		startAt := generateRandomDateTime()
		venue := tg.venues[rand.Intn(len(tg.venues))] // Randomly choose venue
		homeCompetitor := Competitor{ID: generateRandomGUID(), Name: "Home Team"}
		awayCompetitor := Competitor{ID: generateRandomGUID(), Name: "Away Team"}
		lineOdd := Decimal(rand.Float64() * 10) // Random line odd up to 10

		lines[i] = Line{
			LineID:         lineID,
			EventID:        eventID,
			EventType:      eventType,
			StartAt:        startAt,
			Venue:          venue,
			HomeCompetitor: homeCompetitor,
			AwayCompetitor: awayCompetitor,
			LineOdd:        lineOdd,
		}
	}

	return lines
}

// Helper functions to generate random GUID and DateTime

func generateRandomGUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func generateRandomDateTime() time.Time {
	min := time.Date(2022, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0)
}

func main() {
	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Define the list of brokers
	brokers := []string{"localhost:9092"}

	// Topic and partition
	topic := "my-topic"

	// Channel to synchronize completion of all goroutines
	done := make(chan struct{})

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Number of goroutines to launch
	numGoroutines := 15

	// Launch multiple goroutines to produce messages concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Infinite loop to continuously produce messages
			for {
				// Get a random broker
				broker := brokers[rand.Intn(len(brokers))]

				// Generate a random ticket number
				// Create a new TicketGenerator
				ticketGenerator := NewTicketGenerator()

				// Generate a random ticket
				ticket := ticketGenerator.GenerateRandomTicket()

				// Determine the partition based on the ticket number
				partition := 1

				// Dial the leader for the current broker
				conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, partition)
				if err != nil {
					log.Printf("failed to dial leader for broker %s: %v\n", broker, err)
					continue // Skip to the next iteration if this one fails
				}

				// Marshal the ticket into JSON
				value, err := json.Marshal(ticket)
				if err != nil {
					log.Printf("failed to marshal message to JSON: %v\n", err)
					conn.Close() // Close the connection
					continue     // Skip to the next iteration
				}

				// Write the message to Kafka
				_, err = conn.WriteMessages(
					kafka.Message{
						Value: value,
					},
				)
				if err != nil {
					log.Printf("failed to write message for broker %s: %v\n", broker, err)
				}

				// Close the connection
				if err := conn.Close(); err != nil {
					log.Printf("failed to close writer for broker %s: %v\n", broker, err)
				}

			}
		}()
	}

	// Wait for all goroutines to finish
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait for all goroutines to finish
	<-done
}
