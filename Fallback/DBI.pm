#!/usr/bin/perl -w

package Data::Fallback::DBI;

use strict;
use Carp qw(confess);

use DBI;

use Data::Fallback;
use vars qw(@ISA);
@ISA = qw(Data::Fallback);

sub isun {
  return;
}

sub connect {
  my $self = shift;
  my $db = $self->{hash}{db} || $self->{db};
  confess "need a \$db to connect with" unless $db;
  my $dbh = DBI->connect(@{$db});
  confess "couldn't connect with " . join(", ", @{$db}) unless $dbh;
  confess "got a \$DBI::errstr with " . join(", ", @{$db}) . ": $DBI::errstr" if $DBI::errstr;
  return $dbh;
}

sub _GET {
  my $self = shift;

  my $return = 0;
  my $row_hash_ref = $self->get_content;
  if( $row_hash_ref && (defined $row_hash_ref->{$self->{hash}{item}}) && length $row_hash_ref->{$self->{hash}{item}}) {
    $self->{update}{group} = $row_hash_ref;
    $self->{update}{item} = $row_hash_ref->{$self->{hash}{item}};
    $return = 1;
  }

  return $return;
}

sub get_content {
  my $self = shift;

  my ($found_in_cache, $hash_ref, $found_at_cache_level) = 
    $self->check_cache('DBI', 'group', $self->{hash}{content});

  if($found_in_cache) {
    # already set in $hash_ref, so we're done
  } else {
    my $dbh = $self->get_dbh;
    $self->{cache}{DBI} ||= {};
    $self->{cache}{DBI}{sth} ||= {};
    $self->{cache}{DBI}{sth}{$self->{hash}{content}} ||= $dbh->prepare($self->{hash}{content});
    my $primary_key = $self->{hash}{primary_key} || $self->{primary_key};
    my @primary_key = GET_VALUES($primary_key);
    my $execute = $self->{cache}{DBI}{sth}{$self->{hash}{content}}->execute(@primary_key);
    confess "got a \$DBI::errstr with $self->{hash}{content}: $DBI::errstr" if $DBI::errstr;
    $hash_ref = $self->{cache}{DBI}{sth}{$self->{hash}{content}}->fetchrow_hashref('NAME_lc');
    confess "got a \$DBI::errstr with $self->{hash}{content}: $DBI::errstr" if $DBI::errstr;
    $self->set_cache(ref $self, 'group', $self->{hash}{content}, $hash_ref);
  }
  return $hash_ref;
}

sub get_dbh {
  my $self = shift;
  my $dbh = $self->{hash}{dbh} || $self->{dbh};
  unless($dbh) {
    $dbh = $self->connect;
    $self->{dbh} = $self->{hash}{dbh} = $dbh;
  }
  return $dbh;
}

sub SET_ITEM {
  my $self = shift;
  my $dbh = $self->get_dbh;
  my $update_item_sql = $self->get_update_item_sql;
  my $key_name = $self->{hash}{content} . ".$self->{item}";
  $self->{cache}{DBI} ||= {};
  $self->{cache}{DBI}{sql} ||= {};
  $self->{cache}{DBI}{sql}{$key_name} ||= $update_item_sql;
  $self->{cache}{DBI}{sth}{$key_name} ||= $dbh->prepare($self->{cache}{DBI}{sql}{$key_name});
  confess "got a \$DBI::errstr with $self->{cache}{DBI}{sql}{$key_name}: $DBI::errstr" if $DBI::errstr;
  my $primary_key = $self->{hash}{primary_key} || $self->{primary_key};
  my @primary_key = GET_VALUES($primary_key);
  isun "UPDATE'ing with $self->{cache}{DBI}{sql}{$key_name} ($self->{update}{item}, @primary_key)";
  my $execute = $self->{cache}{DBI}{sth}{$key_name}->execute($self->{update}{item}, @primary_key);
  confess "got a \$DBI::errstr with $self->{cache}{DBI}{sql}{$key_name}: $DBI::errstr" if $DBI::errstr;
}

sub SET_GROUP {
  my $self = shift;
  my $dbh = $self->get_dbh;
  my ($table, $where) = $self->get_table_where($self->{hash}{content});
  my $key_name = $self->{hash}{content} . ".there.$self->{item}";
  my $primary_key = $self->{hash}{primary_key} || $self->{primary_key};
  my @primary_key = GET_VALUES($primary_key);
  $self->{cache}{DBI} ||= {};
  $self->{cache}{DBI}{sql} ||= {};
  $self->{cache}{DBI}{sql}{$key_name} ||= $self->{hash}{content};
  $self->{cache}{DBI}{sth}{$key_name} ||= $dbh->prepare($self->{cache}{DBI}{sql}{$key_name});
  $self->{cache}{DBI}{sth}{$key_name}->execute(@primary_key);
  isun $self->{cache}{DBI}{sql}{$key_name}, \@primary_key;
  if(my @array = $self->{cache}{DBI}{sth}{$key_name}->fetchrow_array) {
    # record is there, need to UPDATE
    isun "there";
    unless($self->do_update) {
    }
    exit;
  } else {
    # record is not there, need to SELECT
    confess "got a \$DBI::errstr with $self->{hash}{content}: $DBI::errstr" if $DBI::errstr;
    isun "not there";
    unless($self->do_insert) {
    }
  }
  isun $self->{hash}, $table, $where;
  exit;
}

sub do_insert {
  my $self = shift;
  my $dbh = $self->get_dbh;
  my ($insert_sql, $args) = $self->get_insert_info;
  $self->{cache}{DBI} ||= {};
  $self->{cache}{DBI}{sql} ||= {};
  $self->{cache}{DBI}{sql}{$insert_sql} ||= $insert_sql;
  $self->{cache}{DBI}{sth}{$insert_sql} ||= $dbh->prepare($self->{cache}{DBI}{sql}{$insert_sql});
  isun $self->{cache}{DBI};
  my $inserted = $self->{cache}{DBI}{sth}{$insert_sql}->execute(@{$args});
  confess "got a \$DBI::errstr with $self->{hash}{content}: $DBI::errstr" if $DBI::errstr;
  return $inserted;
}

sub get_insert_info {
  my $self = shift;
  my $table = $self->get_table($self->{hash}{content});
  my ($cols, $values) = ('', '');
  my @args = ();
  foreach(sort keys %{$self->{update}{group}}) {
    $cols .= "$_, ";
    $values .= "?, ";
    push @args, $self->{update}{group}{$_};
  }
  foreach($cols, $values) {
    s/,\s+$//;
  }
  my $insert_sql = "INSERT INTO $table (" . $cols . ") VALUES (" . $values . ")";
  return ($insert_sql, \@args);
}

sub do_update {
  my $self = shift;
  my $dbh = $self->get_dbh;
  my ($update_sql, $args) = $self->get_update_info;
  $self->{cache}{DBI} ||= {};
  $self->{cache}{DBI}{sql} ||= {};
  $self->{cache}{DBI}{sql}{$update_sql} ||= $update_sql;
  $self->{cache}{DBI}{sth}{$update_sql} ||= $dbh->prepare($self->{cache}{DBI}{sql}{$update_sql});

  my $primary_key = $self->{hash}{primary_key} || $self->{primary_key};
  my @primary_key = GET_VALUES($primary_key);

  my $updated = $self->{cache}{DBI}{sth}{$update_sql}->execute(@{$args}, @primary_key);
  confess "got a \$DBI::errstr with $self->{hash}{content}: $DBI::errstr" if $DBI::errstr;
  return $updated;
}

sub get_update_info {
  my $self = shift;
  my ($table, $where) = $self->get_table_where($self->{hash}{content});
  isun $table, $where;
  my ($cols, $values) = ('', '');
  my @args = ();
  my $update_sql = "UPDATE $table SET ";
  foreach(sort keys %{$self->{update}{group}}) {
    $update_sql .= "$_ = ?, ";
    push @args, $self->{update}{group}{$_};
  }
  $update_sql =~ s/,\s+$//;
  isun $update_sql;
  $update_sql .= " WHERE " . $where;
  return ($update_sql, \@args);
}

sub get_update_item_sql {
  my $self = shift;

  my ($table, $where) = $self->get_table_where($self->{hash}{content});
  my $update_item_sql = "UPDATE $table SET $self->{item} = ? WHERE $where"; 
  return $update_item_sql;
}

sub get_table_where {
  my $self = shift;
  my $sql = shift;
  my $table = $self->get_table($sql);
  my $where = $self->get_where($sql);
  return ($table, $where);
}

sub get_table {
  my $self = shift;
  my $sql = shift;
  isun $sql;
  my $table;
  if($sql =~ /^\s*select\s+.+?\s+from\s+([a-z0-9_]+)\s+where/si) {
    $table = $1;
  } else {
    die "couldn't find a table name easily in $sql";
  }
  return $table;
}

sub get_where {
  my $self = shift;
  my $sql = shift;
  my $where;
  if($sql =~ /^\s*select\s+.+?\s+from\s+[a-z0-9_]+\s+where\s+(.+)/si) {
    $where = $1;
  } else {
    die "couldn't get your update where easily from $sql";
  }
  return $where;
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
