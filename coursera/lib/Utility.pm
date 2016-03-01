package Utility;

use strict;
use warnings;

##
#
# Author : Muthu Kumar C
# Created in May, 2014
#
##

sub merge_hashes{
	my ($tohash, $fromhash) =@_;
	
	foreach my $fromkey ( keys %{$fromhash} ){
		if( !exists $tohash->{$fromkey} ){
			$tohash->{$fromkey} = $fromhash->{$fromkey};
		}
	}
	return $tohash;
}

sub logg{
	my( $FH, $msg ) = @_;
	print $FH "$msg\n";
}


local $SIG{__WARN__} = sub {
	my $message = shift;
	logg($message);
};


sub fixEOLspaces{
	my($in, $out) = @_;
	while(<$in>){
		chomp($_);
		$_ =~ s/\s*$//g;
		print $out "$_\n";
	}
}

sub copy_array{
	my ($source) = @_;	
	my @destination = ();
	
	foreach my $element(@{$source}){
		push @destination,$element;
	}
	
	return \@destination;
}

sub exit_script{
	my ($progname, $argsarray) = @_;
	print "\n perl $progname \t";
	 foreach (@$argsarray){
		print "$_\t";
	 }
	print "\n has ended";
}