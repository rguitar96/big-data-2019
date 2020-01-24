#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library("shiny")
library(dplyr)
library(ggplot2)
library("plotly")

#Loading the data.
data<-read.csv(file = "../data/clean-twitter-data.csv")

#Creating a column that joins the date and time.
data<-mutate(data, times = paste(Date, Hour))


#Counting the number of tweets per timestamp.
time.series<-plyr::count(select(data, c(times, Date)))
times<-(time.series$times)
time.format<- "%Y-%m-%d %H:%M"
timestamp.formatted <-(as.POSIXct(times, format=time.format)) 
time.series$times<- timestamp.formatted
time.series$Date<- as.Date(time.series$Date)

names(time.series) <- c("Time", "Date", "Tweets")

ggplot(data = time.series, aes(x = Time, y = Tweets, group = 1))+
  geom_line(color = "#00AFBB", size = 0.1)+
  labs(x = "Time", y = "Number of tweets", title = "Number of tweets posted at a given time")


ui <- fluidPage(
  titlePanel("Tweets Time Evolution"),
  
  sidebarLayout(
    sidebarPanel(
      helpText("Select a time interval to examine."),
      
      sliderInput("slider1", label = h3("Date"),
                  min = min(time.series$Date), max = max(time.series$Date),
                  value = c(min(time.series$Date),max(time.series$Date))),
      
      br(),
      br()
    ),
    mainPanel(plotlyOutput("plot")
    )
                        
  )
)

server <- function(input, output) {

  get.data <- reactive({
    min<-as.Date(input$slider1[1])
    max<-as.Date(input$slider1[2])
    
    time.series<-time.series%>%filter(Date>=min & Date<=max)

    times<-(time.series$Time)
    time.format<- "%Y-%m-%d %H:%M"
    times.formatted <-(as.POSIXct(times, format=time.format)) 
    time.series$Time<- times.formatted
    return(as.data.frame(time.series))
  })
    
  
  output$plot <- renderPlotly({
    ggplot(data = get.data(), aes(x = Time, y = Tweets, group = 1))+
      geom_line(color = "#00AFBB", size = 0.1)+
      labs(x = "Time", y = "Number of tweets", title = "Number of tweets posted at a given time")
  })
  
}

shinyApp(ui = ui, server = server)

