#!/usr/bin/perl -w

package Data::Fallback::ConfFile;

use strict;

use Data::Fallback;
use vars qw(@ISA);
@ISA = qw(Data::Fallback);

sub _GET {
  my $self = shift;

  my $return = 0;

  my ($found_in_cache, $content) = 
    $self->check_cache('ConfFile', 'item', $self->{item});

  if($found_in_cache) {
    $self->{update}{item} = $content;
    $return = 1;
  } else {
    my $contents = $self->get_content;
    my $from_file_hash = contentToHash(\$contents);
    if( $from_file_hash && (defined $from_file_hash->{$self->{hash}{item}}) && length $from_file_hash->{$self->{hash}{item}}) {
      $self->{update}{group} = $from_file_hash;
      $self->{update}{item} = $from_file_hash->{$self->{hash}{item}};
      $self->set_cache('ConfFile', 'item', $self->{item}, $self->{update}{item});
      $return = 1;
    }
  }

  return $return;
}

sub SET_ITEM {
  my $self = shift;
  if($self->{hash}{content} && -e $self->{hash}{content}) {
    my $content = Include($self->{hash}{content});
    my $file_hash = contentToHash(\$content);
    unless( (defined $file_hash->{$self->{item}}) && $file_hash->{$self->{item}} eq $self->{update}{item}) {
      $file_hash->{$self->{item}} = $self->{update}{item};
      write_conf_file($self->{hash}{content}, $file_hash);
    }
  }
}

sub SET_GROUP {
  my $self = shift;
  return write_conf_file($self->{hash}{content}, $self->{update}{group});
}

sub get_content {
  my $self = shift;

  my ($found_in_cache, $content) = 
    $self->check_cache('ConfFile', 'group', $self->{hash}{content});

  if($found_in_cache) {
    # already set in $content, so we're done
  } elsif(-e $self->{hash}{content}) {
    $content = Include($self->{hash}{content});
    $self->set_cache('ConfFile', 'group', $self->{hash}{content}, $content);
  } else {
    # no value, no file => do nothing
  }
  return $content;
}

sub contentToHash {
  my $text_ref = shift;
  my %hash = $$text_ref =~ /(.+?)\s+(.+)/g;
  return \%hash;
}

sub hashToContent {
  my $hash_ref = shift;
  my $content = '';
  foreach(sort keys %{$hash_ref}) {
    next unless($hash_ref->{$_});
    $content .= "$_     $hash_ref->{$_}\n";
  }
  return $content;
}

sub Include {
  my $filename = shift;
  return unless(-e $filename);
  open(FILE, $filename);
  my $content = join("", <FILE>);
  close(FILE);
  return $content;
}

sub write_conf_file {
  my ($filename, $hash_ref) = @_;
  my $txt = hashToContent($hash_ref);
  open (FILE, ">$filename");
  print FILE $txt;
  close(FILE);
}

1;
