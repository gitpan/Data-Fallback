#!/usr/bin/perl -w

package Data::Fallback::Daemon;


use strict;

use vars qw(@ISA);
use Net::Server::PreForkSimple;
use Data::Dumper;
use Data::Fallback;
use Time::HiRes qw(gettimeofday);

@ISA = qw(Net::Server::PreForkSimple);

### over-ridden subs below

sub new {
  my $type = shift;
  my $hash_ref = $_[0];
  my @PASSED_ARGS = (ref $hash_ref eq 'HASH') ? %{$_[0]} : @_;
  my $cache_object;
  my @DEFAULT_ARGS = (
  );
  my %ARGS = (@DEFAULT_ARGS, @PASSED_ARGS);
  my $self = bless \%ARGS, $type;
  return $self;
}

sub run {
  my $self = shift;
  $self->SUPER::run(max_servers => 1,
                    max_requests => 100000,  
reverse_lookups => 0,
                    );
  
}


sub __configure_hook {
  my $self = shift;
  $self->{fallback} ||= Data::Fallback->new({
  });
  $self->{fallback}{loaded_list} ||= {};
}

sub process_request {
  my $self = shift;
  $self->{start} = gettimeofday;
  eval {

    $self->{cookies} = {
      elapsed => 1,
      history => 1,
    };
    $self->{output} = "";

    local $SIG{ALRM} = sub { die "Timed Out!\n" };
    my $timeout = 30;
    my $previous_alarm = alarm($timeout);
    my $first_line = <STDIN>;

    # GET /$list/$primary_key/$item


#    GET /test/14/foo HTTP/1.1

                   #  method
    $first_line =~ m@^(\w+)\s+
    
    # list name
      /(\w+)
    # primary_key
      /(\w+)
    # column
      /([\w,]+)@x;

    my ($method, $list_name, $primary_key, $item) = ($1, $2, $3, $4);
    if($method eq 'GET') {
      unless($self->{fallback}{loaded_list}{$list_name}) {
        $self->{output} .= "Unknown list name: $list_name\n";
        $self->{output} .= "Known lists: " . join(", ", keys %{$self->{fallback}{loaded_list}}) . "\n";
      }
      #$self->{output} .= Dumper $self;
      #$self->{output} .= Dumper $first_line;
      #$self->{output} .= Dumper $method;
      #$self->{output} .= Dumper $list_name;
      #$self->{output} .= Dumper $primary_key;
      #$self->{output} .= Dumper $item;

      $self->{fallback}{list} = $self->{fallback}{loaded_list}{$list_name};
      $self->{fallback}{list_name} = $list_name;
      $self->{fallback}{hash}{primary_key} = $primary_key if(defined $primary_key);
      #$self->{output} .= Dumper $self->{fallback}{hash}{primary_key};
      my $get = $self->{fallback}->get("/$primary_key/$item");
      if(ref $get && ref $get eq 'ARRAY') {
        $self->{output} .= "<type>array</type>\n";
        foreach(@{$get}) {
          $self->{output} .= "<array>$_</array>\n";
        }
      } else {
        $self->{output} .= "<type>scalar</type>\n";
        $self->{output} .= "<scalar>$get</scalar>\n";
      }
    } else {
      $self->{output} .= "Unknown method: $method, please use GET ($first_line)\n";
    }

    alarm($timeout);
    while( <STDIN> ){
      s/\r?\n$//;
      last if(!$_ || $_ eq "");
      if(/^(?:Set-)?Cookie: (.+?)\s*=\s*(.+?)(?:;|$)/g) {
        $self->{cookies}{$1} = $2;
      } else {
        #$self->{output} .= "You said \"$_\"\r\n";
      }
      alarm($timeout);
    }
    alarm($previous_alarm);

  };

  if( $@ ){
    if($@=~/timed out/i ){
      $self->{output} .= "Timed Out.\n";
    } else {
      $self->{output} .= "<error>$@</error>\n";
    }
  }
  $self->out;
  return;
}

sub print_header {
  my $self = shift;
  print "HTTP/1.0 200\r\n";
  print "Content-type: text/plain\r\n\r\n";
}

sub handle_cookies {
  my $self = shift;
  foreach(keys %{$self->{cookies}}) {
    next unless($self->{cookies}{$_});
    if($_ =~ /history/) {
      $self->{output} .= "<history>\n" . Dumper($self->{fallback}{history}) . "</history>\n";
    }
  }
}

sub out {
  my $self = shift;
  $self->print_header;
  $self->handle_cookies if(scalar keys %{$self->{cookies}});
  $self->{output} .= "<elapsed>" . (gettimeofday - $self->{start}) . "</elapsed>\n" if($self->{cookies}{elapsed});
  $self->{output} =~ s/([^\r\n]?)\n/$1\r\n/g;
  print $self->{output};
}

sub GET_VALUES {
  my $values=shift;
  return () unless defined $values;
  if (ref $values eq "ARRAY") {
    return @$values;
  }
  return ($values);
}

=head1 NAME

Data::Fallback::Daemon - a daemon for Data::Fallback

=head1 DESCRIPTION

Data::Fallback works great at finding data at the fastest place you tell it, but what happens when you keep
asking for the same data repeatedly?  Well, Data::Fallback::Daemon allow to cache in memory the data, and a simple
protocol for retrieving it.

=head1 TYPICAL USAGE

Having one database box and n client boxes doesn't scale too hot for n very large.  However, one database box and n
client boxes each running their own Data::Fallback::Daemon scales well for large n.  So, typical usage in my view is
to have local daemons running on each client box.

=head1 EXAMPLE

  #!/usr/bin/perl -w

  use strict;
  use Data::Fallback;
  use Data::Fallback::Daemon;

  # first we set up a simple Data::Fallback::Daemon object
  my $self = Data::Fallback::Daemon->new({
    # reverse_lookups can take awhile when you aren't connected to the web,
    # so I just turn them off for testing purposees
    reverse_lookups => '',

    # we need to include a Data::Fallback object
    fallback => Data::Fallback->new(),
  });

  my $db1_dsn = ['dbi:mysql:CHANGE TO YOUR DATABASE', 'CHANGE TO YOUR USER', 'CHANGE TO YOUR PASSWORD'];
  my $db2_dsn = ['dbi:Pg:dbname=CHANGE TO YOUR DATABASE', 'CHANGE TO YOUR USER', 'CHANGE TO YOUR PASSWORD'];

  # loaded_list is a hash ref of all the lists you want to have the daemon maintain
  # the hash keys two_dbs, second_test are how the lists are referenced
  $self->{fallback}{loaded_list} = {
    db_to_conf => [
      {
        content       => '/tmp/fallback/$primary_key',
        package       => 'ConfFile',
        accept_update => 'group',
      },
      {
        db      => $db1_dsn,
        content => 'SELECT foo FROM foo WHERE id = ?',
        package => 'DBI',
      },
    ],
    two_dbs => [
      {
        db      => $db1_dsn,
        content => 'SELECT foo FROM foo WHERE id = ?',
        package => 'DBI',
      },
      {
        db      => $db2_dsn,
        content => 'SELECT foo FROM foo WHERE id = ?',
        package => 'DBI',
      },
    ],
  };

  # all set up, just need to run
  $self->run();

1;
