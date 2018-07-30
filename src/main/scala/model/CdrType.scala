package model

object CdrType extends Enumeration{
  type Type = Value
  val ADJ, CLR, CM, COM, DATA, FIRST, MMS, MON, SMS, VOICE, VOU = Value
}