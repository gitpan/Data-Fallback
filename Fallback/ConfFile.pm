#!/usr/bin/perl -w

package Data::Fallback::ConfFile;

use strict;

use Data::Fallback;
use vars qw(@ISA);
@ISA = qw(Data::Fallback);

sub get_conffile_filename {
  my $self = shift;
  
  # allows for /tmp/fallback/$primary_key
  # to cache information for numerous keys like 1, 2, 7 for
  # SELECT * FROM foo WHERE key = ?
  # for example
  my $primary_key = $self->get_cache_key('primary_key');

  my $return = $self->{hash}{content};
  $return =~ s/\$primary_key/$primary_key/g if($primary_key);
  return $return;
}

sub _GET {
  my $self = shift;

  my $return = 0;

  my $key = $self->get_conffile_filename . ".$self->{item}";

  my ($found_in_cache, $content) = 
    $self->check_cache('ConfFile', 'item', $key);

  if($found_in_cache) {
    $self->{update}{item} = $content;
    $return = 1;
  } else {
    my $contents = $self->get_content;
    my $from_file_hash = contentToHash(\$contents);
    if( $from_file_hash && (defined $from_file_hash->{$self->{hash}{item}}) && length $from_file_hash->{$self->{hash}{item}}) {
      $self->{update}{group} = $from_file_hash;
      $self->{update}{item} = $from_file_hash->{$self->{hash}{item}};
      $self->set_cache('ConfFile', 'item', $self->get_conffile_filename . ".$self->{hash}{item}", $self->{update}{item});
      $return = 1;
    }
  }

  return $return;
}

sub SET_ITEM {
  my $self = shift;
  my $filename = $self->get_conffile_filename;
  if($filename && -e $filename) {
    my $content = Include($filename);
    my $file_hash = contentToHash(\$content);
    unless( (defined $file_hash->{$self->{item}}) && $file_hash->{$self->{item}} eq $self->{update}{item}) {
      $file_hash->{$self->{item}} = $self->{update}{item};
      write_conf_file($filename, $file_hash);
    }
  }
}

sub SET_GROUP {
  my $self = shift;
  return write_conf_file($self->get_conffile_filename, $self->{update}{group});
}

sub get_content {
  my $self = shift;

  my $filename = $self->get_conffile_filename;
  my ($found_in_cache, $content) = 
    $self->check_cache('ConfFile', 'group', $filename);

  if($found_in_cache) {
    # already set in $content, so we're done
  } elsif(-e $filename) {
    $content = Include($filename);
    $self->set_cache('ConfFile', 'group', $filename, $content);
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
