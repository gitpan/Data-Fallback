#!/usr/bin/perl -w

package Data::Fallback::Daemon;

use strict;

use vars qw(@ISA);
use Net::Server::PreForkSimple;
use Dumper;
use Data::Fallback;
use Time::HiRes qw(gettimeofday);

@ISA = qw(Net::Server::PreForkSimple);

### over-ridden subs below

sub run {
  my $self = shift;
  $self->SUPER::run(max_servers => 5,
                    max_requests => 100000,  
                    );
  
}


sub configure_hook {
  my $self = shift;
  $self->{fallback} = Data::Fallback->new({
    db          => ["DBI:mysql:fb:localhost", 'earl', 'earl'],
    loaded_list => {
      'test' => [
        {
          accept_update => 'group',
          cache_level => 'all',
          content => '/tmp/over',
        },
        {
          cache_level => 'all',
          content     => '/tmp/default',
        },
        {
          primary_key => 1,
          content     => "SELECT * FROM fallback_test WHERE id = ?",
          package     => 'DBI',
        },
        {
          cache_level => 0,
          primary_key => 1,
          content     => "SELECT * FROM fallback_test2 WHERE id = ?",
          package     => 'DBI',
        },
      ],
    },
    package => 'ConfFile',
    zeroth_hash => {
      ttl => '1 minute',
    },
  });
  $self->{fallback}{loaded_list} ||= {};
}

sub process_request {
  my $self = shift;
  $self->{start} = gettimeofday;
  eval {

    $self->{cookies} = {
      #elapsed => 1,
      #history => 1,
    };
    $self->{output} = "";

    local $SIG{ALRM} = sub { die "Timed Out!\n" };
    my $timeout = 30;
    my $previous_alarm = alarm($timeout);
    my $first_line = <STDIN>;

    $first_line =~ m@^(\w+)\s+/(\w+)/([\w,]+)@;

    my ($method, $list_name, $key) = ($1, $2, $3);
    if($method eq 'GET') {
      unless($self->{fallback}{loaded_list}{$list_name}) {
        $self->{output} .= "Unknown list name: $list_name\n";
        $self->{output} .= "Known lists: " . join(", ", keys %{$self->{fallback}{loaded_list}}) . "\n";
      }
      $self->{fallback}{list} = $self->{fallback}{loaded_list}{$list_name};
      $self->{fallback}{list_name} = $list_name;
      my $get = $self->{fallback}->get($key);
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
      $self->{output} .= "Unknown method: $method, please use GET\n";
    }

    alarm($timeout);
    while( <STDIN> ){
      s/\r?\n$//;
      last if($_ eq "");
      if(/^(Set-)?Cookie: (.+?)\s*=\s*(.+?)(;|$)/g) {
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

1;
