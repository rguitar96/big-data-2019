library(shiny)
library(tm)
library(wordcloud)
library(wordcloud2)
library(memoise)
library(dplyr)


languages = data.frame(
              name = c("Spanish",
                        "English",
                       "Portuguese", 
                       "Danish",
                       "French",
                       "Italian",
                       "German",
                       "Turkish",
                       "Dutch",
                       "Hungarian",
                       "Finnish",
                       "Russian",
                       "Swedish"),
             code = c("es", 
                          "en", 
                          "pt",
                          "da", 
                          "fr",
                          "it",
                          "de",
                          "tr",
                          "nl", 
                          "hu", 
                          "fi", 
                          "ru", 
                          "sv"))


#Using "memoise" to automatically cache the results
getTermMatrix <- memoise(function(langName, startDate, endDate) {
  
  lang = languages %>% filter(name == langName)
                       
  
  text <-data %>%
    filter(`Tweet language (ISO 639-1)` == lang$code) %>% 
    filter(Date >= startDate) %>% 
    filter(Date <= endDate) %>% 
    select(`Tweet content`) 
  
    docs = Corpus(VectorSource(text))
    
    toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))
    docs <- tm_map(docs, toSpace, "/")
    docs <- tm_map(docs, toSpace, "@")
    docs <- tm_map(docs, toSpace, "\\|")
    
    # Convert the text to lower case
    docs <- tm_map(docs, content_transformer(tolower))
    # Remove numbers
    docs <- tm_map(docs, removeNumbers)
    # Remove language common stopwords
    docs <- tm_map(docs, removeWords, stopwords(tolower(lang$name)))
    # Remove punctuations
    docs <- tm_map(docs, removePunctuation)
    # Eliminate extra white spaces
    docs <- tm_map(docs, stripWhitespace)
    # Remove your own stop word
    # specify your stopwords as a character vector
    docs <- tm_map(docs, removeWords, c("https", "tco", "...", "com"))
    
    dtm <- TermDocumentMatrix(docs)
    m <- as.matrix(dtm)
    v <- sort(rowSums(m),decreasing=TRUE)
    d <- data.frame(word = names(v),freq=v)
})



ui <-  fluidPage(
  
  titlePanel("Tweets WordCloud"),
  
  sidebarLayout(
    
    sidebarPanel(
      selectInput("lang", "Choose a language:",
                  choices = languages$name),
      sliderInput("dateSlider", label = h3("Date"),
                    min = min(data$Date), max = max(data$Date),
                    value = c(min(data$Date),max(data$Date))),
      actionButton("update", "Change")
    ),
    
    mainPanel(
      wordcloud2Output("plot")
    )
  )
)

# Define server logic required to draw a histogram ----
server <- function(input, output) {
  
              # Define a reactive expression for the document term matrix
              terms <- reactive({
                # Change when the "update" button is pressed...
                input$update
                # ...but not for anything else
                isolate({
                  withProgress({
                    setProgress(message = "Processing corpus...")
                    getTermMatrix(input$lang, input$dateSlider[1], input$dateSlider[2])
                  })
                })
              })
              
              
              output$plot <- renderWordcloud2({
                wordcloud2(data=terms(), size = 0.7, shape = 'pentagon')
              })
              }


shinyApp(ui = ui, server = server)