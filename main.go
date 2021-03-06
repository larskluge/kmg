package main

import (
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/dustin/go-humanize"
	ui "github.com/gizak/termui"
)

var (
	brokersFlag = flag.String("kafka-brokers", "127.0.0.1:9092,kafka:9092", "Kafka Brokers, separate multiple with ','")
	headerRow   = []string{"Topic", "Partitions", "Estimated Messages", "Total Offset", "Growth"}
	sepRow      = []string{"─", "─", "─", "─", "─"}
	headers     = [][]string{headerRow, sepRow}
	offsets     = map[string][5]int64{}
)

func main() {
	flag.Parse()

	brokers := strings.Split(*brokersFlag, ",")

	cfg := sarama.NewConfig()
	cfg.ClientID = "kmg"

	client, err := sarama.NewClient(brokers, cfg)
	check(err)
	defer client.Close()

	err = ui.Init()
	check(err)
	defer ui.Close()

	table := ui.NewTable()
	table.Border = false
	table.Separator = false
	table.FgColor = ui.ColorWhite
	table.BgColor = ui.ColorDefault
	table.TextAlign = ui.AlignRight

	draw := func() {
		table.Rows = Rows(client)
		table.FgColors = []ui.Attribute{}
		table.BgColors = []ui.Attribute{}
		table.Analysis()
		table.SetSize()
		ui.Render(table)
	}

	draw()

	ui.Handle("/timer/1s", func(e ui.Event) {
		draw()
	})

	ui.Handle("/sys/kbd/q", func(ui.Event) {
		ui.StopLoop()
	})
	ui.Loop()
}

type ByTopicName [][]string

func (a ByTopicName) Len() int           { return len(a) }
func (a ByTopicName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTopicName) Less(i, j int) bool { return a[i][0] < a[j][0] }

func Rows(client sarama.Client) [][]string {
	rows := [][]string{}

	topics, err := client.Topics()
	check(err)

	for _, topic := range topics {
		row := []string{topic}

		partitions, err := client.Partitions(topic)
		check(err)

		row = append(row, strconv.Itoa(len(partitions)))

		var offset, count int64 = 0, 0
		for _, partition := range partitions {
			offsetNewest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			check(err)

			offsetOldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
			check(err)

			offset += offsetNewest
			count += offsetNewest - offsetOldest
		}

		row = append(row, fmt.Sprintf("%18s", humanize.Comma(count)))
		row = append(row, fmt.Sprintf("%18s", humanize.Comma(offset)))

		// msg/s calculation
		to := offsets[topic]
		before := to[len(to)-1]
		to[1], to[2], to[3], to[4] = to[0], to[1], to[2], to[3]
		to[0] = offset
		offsets[topic] = to
		msgs := "?"
		if before > 0 {
			diff := (offset - before) / int64(len(to))
			msgs = humanize.Comma(diff)
		}
		row = append(row, fmt.Sprintf("%12s msg/s", msgs))

		rows = append(rows, row)
	}
	sort.Sort(ByTopicName(rows))

	return append(headers, rows...)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
