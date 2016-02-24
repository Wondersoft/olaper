# Olaper

Olaper is the XMLA OLAP interface to your database. It allows to run multi-dimensional (MDX) queries
on your database - be it MySQL, Oracle, Vertica or other. You then can use specialized OLAP
tools, like Saiku[http://www.meteorite.bi/products/saiku] to see your data in different dimensions
and perform deep analysis.

Olaper is installed as Java servlet in the J2EE container, like Jetty[http://download.eclipse.org/jetty/] or
Tomcat[http://tomcat.apache.org/]. Then it should be configured according to the schema of your database.


![olaper usage schema](https://raw.githubusercontent.com/Wondersoft/olaper/master/content/olaper.png)


## Installation

Add this line to your application's Gemfile:

```ruby
gem 'olap-xmla'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install olap-xmla

## Usage

Using the gem is very simple, only basic knowledge on OLAP is required.

### Connecting to server

To use the gem, you need to know the connection requisites to connect to XMLA server:

1. Server URL ( typically, http: or https: URL )
2. Datasource and catalog names. You can check them in your XMLA server configuration

Connecting to the server and executing MDX are straightforward:

```ruby
require 'olap/xmla'

client = Olap::Xmla.client({
            server: 'http://your-olap-server',
            datasource: 'your-datasource',
            catalog: 'your-catalog'})
response = client.request 'your-mdx-here'
```

### Configuration in Rails

If you are using this gem in Rails application, which uses just single OLAP data source,
you can simplify the code by pre-configuring the XMLA connection.

Create a file olap.rb in config/initializers directory with the following content:

```ruby
Olap::Xmla.default_options= { server: 'http://your-olap-server',
                                datasource: 'your-datasource',
                                catalog: 'your-catalog'}
```

Then in Rails application code you can simply do:

```ruby
response = Olap::Xmla.client.request 'your-mdx-here'
```

### Querying MDX

The gem does not parse MDX, just passes it to XMLA server.

However, it can do substituting parameters in the query:

```ruby
MDX_QUERY = 'SET [~ROWS_Date] AS {[DateTime].[Date].[Date].[%DATE%]}'

Olap::Xmla.client.request MDX_QUERY, {'%DATE%' => '20150530'}
```

This allows to store MDX queries in constants, while execute them with dynamic parameters.
Note, that you should never use these parameters directly from Rails request, as
this may create security breach!

### Response


You may use the response to render the results to user, or post-process it to analyse the data
The following methods can be used to request the meta-data and data from the response:

```ruby
response = client.request(mdx)


# Meta - data of the response
response.measures  #  array of the columns definitions ( :name / :caption )
response.dimensions # array of the rows definitions ( :name )

# Response data
response.rows # rows of the response
response.to_hash # response as a hash
response.column_values(column_num) # just one column of the response

```

### Example: rendering a table on web page

Typically, the request should be done in controller action, as simple as:

OlapController.erb
```ruby
def index
@response = Olap::Xmla.client.request 'WITH SET ... your mdx goes here';
%>
```

and in the HTML Erb view you use iteration over the response as:

index.html.erb
```ruby
<table>
  <thead><tr>
        <% for dim in @response.dimensions %><th><%= dim[:name] %></th><% end %>
        <% for m in @response.measures %><th><%= m[:caption] %></th><% end %>
    </tr></thead>
  <tbody>
    <% for row in @response.rows %>
    <tr>
       <% for label in row[:labels] %>
        <td><%= label[:value] %></td>
       <% end %>
      <% for value in row[:values] %>
          <td><%= value[:fmt_value] || value[:value] %></td>
      <% end %>
    </tr>
    <% end %>
  </tbody>
</table>
```

Have fun!

## Contributing

1. Fork it ( https://github.com/Wondersoft/olap-xmla/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request






mysql> create database foodmart  default character set utf8 default collate utf8_general_ci;
Query OK, 1 row affected (0,01 sec)

mysql> create user 'foodmart'@'localhost' identified by 'foodmart';
Query OK, 0 rows affected (0,11 sec)

mysql> grant ALL privileges on foodmart.* to 'foodmart'@'localhost' identified by 'foodmart' with grant option;
Query OK, 0 rows affected (0,00 sec)

checkout the project https://github.com/OSBI/foodmart-data


MBP-Aleksej:foodmart-data-master studnev$ cd data/
MBP-Aleksej:data studnev$ unzip DataScript.zip 
Archive:  DataScript.zip
  inflating: FoodMartCreateData.sql  
MBP-Aleksej:data studnev$ ls
DataScript.zip		FoodMartCreateData.sql
MBP-Aleksej:data studnev$ cd ..
MBP-Aleksej:foodmart-data-master studnev$ sh FoodMartLoader.sh --db mysql