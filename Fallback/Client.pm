#!/usr/bin/perl -w

package Data::Fallback::Client;

use strict;
use Carp qw(confess);
use IO::Socket;
use Time::HiRes qw(gettimeofday);

use Data::Fallback;
use vars qw(@ISA);
@ISA = qw(Data::Fallback);

sub new {
  my $type  = shift;
  my $class = ref($type) || $type || __PACKAGE__;
  my @PASSED_ARGS = (ref $_[0] eq 'HASH') ? %{$_[0]} : @_;
  my @DEFAULT_ARGS = (
    reverse_lookup => 0,
    host => 'localhost',
    port => '20203',
  );
  my %ARGS = (@DEFAULT_ARGS, @PASSED_ARGS);
  return $class->SUPER::new(\%ARGS);
}

sub get {
  my $self = shift;
  $self->{get_this} = shift;
  die "need a \$self->{get_this} on the get" unless($self->{get_this});
  my $start = gettimeofday;
  $self->get_socket;
  print "<elapsed>" . (gettimeofday - $start) . "</elapsed>\n";
  $self->make_block;
  $self->post_block;
  $self->get_response;
  print "repsonse\n$self->{response}{body}\n";
  return $self->parse_response;
}

sub get_socket {
  my $self = shift;
  unless($self->{socket}) {
    $self->{socket} = new IO::Socket::INET (
      Proto    => "tcp",
      PeerAddr => $self->{host},
      PeerPort => $self->{port},
    );
  }
}

sub make_block {
  my $self = shift;
  $self->{block} = "GET $self->{get_this}\n";
  $self->append_cookies;
  $self->{block} .= "\n";
}

sub append_cookies {
  my $self = shift;
  return unless($self->{cookies} && scalar keys %{$self->{cookies}});
  foreach(keys %{$self->{cookies}}) {
    $self->{block} .= "Cookie: $_=$self->{cookies}{$_}\n";
  }
}

sub post_block {
  my $self = shift;
  $self->{block} =~ s/([^\r\n]?)\n/$1\r\n/g;
  my $socket = $self->{socket};
  print $socket $self->{block};
}

sub get_response {
  my $self = shift;
  $self->{response} = {};
  $self->{response}{body} = "";

  my $socket = $self->{socket};
  $self->{response}{header} = "";
  for(1 .. 3) {
    $self->{response}{header} .= <$socket>;
  }

  while(<$socket>) {
    s/\r//;
    $self->{response}{body} .= $_;
  }
}

sub parse_response {
  my $self = shift;
  my ($type) = $self->{response}{body} =~ m@^<type>(\w+)</type>@;
  my $return;
  if($type eq 'scalar') {
    ($return) = $self->{response}{body} =~ m@<scalar>(.+?)</scalar>@s;
  } elsif($type eq 'array') {
    $return = [];
    while($self->{response}{body} =~ m@<array>(.+?)</array>@sg) {
      push @{$return}, $1;
    }
  }
  return $return;
}

1;
